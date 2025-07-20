/* Copyright (c) 2001-2005 Python Software Foundation.  All rights reserved.

   This code is released under the standard PSF license.
   See the file LICENSE.
*/

/* Python wrapper module for the Spread toolkit: http://www.spread.org/ */

#define PY_SSIZE_T_CLEAN
#include "Python.h"
#include "structmember.h"
#include "sp.h"

#ifdef WITH_THREAD
/*
Jonathan Stanton (of Spread) verified multithreaded apps can suffer races
when Spread disconnects a mailbox, due to socket recycling (another thread,
which doesn't know about the disconnect, continues using the same socket
desciptor, but it can magically refer to a new connection).  They know
about this, and believe they know how to fix it, but haven't fixed it yet
(3.16.2).

We used to worm around it by setting the mbox disconnected flag to true when
Spread returns CONNECTION_CLOSED or ILLEGAL_SESSION.  Guido scoured the Spread
source and determined that those were the only Spread errors that closed
the socket descriptor.  Alas, it takes another level of locking to do the
{check that flag, call Spread, maybe set that flag} sequence indivisibly.

However that also has the effect of serializing all calls to Spread via a
given mailbox, and so a call to receive() in one thread blocks all threads
from invoking Spread on that mailbox until the receive() returns.  But if
the receive is waiting for a multicast from another thread, that's deadlock.

Until Spread disconnection semantics are fixed, we can't win:  we either
leave the Spread disconnection race unaddressed, or leave an app open to
deadlock.  For now we choose the former.
*/
#include "pythread.h"
/* #define SPREAD_DISCONNECT_RACE_BUG */
#endif

#ifdef SPREAD_DISCONNECT_RACE_BUG
/* Acquiring the lock is ugly, because another thread may be holding on to
   it:  we need to release the GIL in order to allow the other thread to
   make progress.
*/
#define ACQUIRE_MBOX_LOCK(MBOX)					\
	do { 							\
		Py_BEGIN_ALLOW_THREADS				\
		PyThread_acquire_lock((MBOX)->spread_lock, 1);	\
		Py_END_ALLOW_THREADS				\
	} while(0)

#define RELEASE_MBOX_LOCK(MBOX) PyThread_release_lock((MBOX)->spread_lock)

#else
#define ACQUIRE_MBOX_LOCK(MBOX)
#define RELEASE_MBOX_LOCK(MBOX)
#endif

static PyObject *SpreadError;

#define DEFAULT_GROUPS_SIZE 10
#define DEFAULT_BUFFER_SIZE 10000

typedef struct {
	PyObject_HEAD
	mailbox mbox;
	PyObject *private_group;
	int disconnected;
#ifdef SPREAD_DISCONNECT_RACE_BUG
	PyThread_type_lock spread_lock;
#endif
} MailboxObject;

static PyObject *spread_error(int, MailboxObject *);

typedef struct {
	PyObject_HEAD
	PyObject *sender;
	PyObject *groups;
	int msg_type;
	int endian;
	PyObject *message;
} RegularMsg;

typedef struct {
	PyObject_HEAD
	int reason;
  int msg_subtype;
	PyObject *group;
	PyObject *group_id;
	PyObject *members;
	PyObject *extra; /* that are still members */
  PyObject *changed_member;
} MembershipMsg;

typedef struct {
	PyObject_HEAD
	group_id gid;
} GroupId;

static PyTypeObject Mailbox_Type;
static PyTypeObject RegularMsg_Type;
static PyTypeObject MembershipMsg_Type;
static PyTypeObject GroupId_Type;

#define MailboxObject_Check(v)	(Py_TYPE(v) == &Mailbox_Type)
#define RegularMsg_Check(v)	(Py_TYPE(v) == &RegularMsg_Type)
#define MembershipMsg_Check(v)	(Py_TYPE(v) == &MembershipMsg_Type)
#define GroupId_Check(v)	(Py_TYPE(v) == &GroupId_Type)

static PyObject *
new_group_id(group_id gid)
{
	GroupId *self;

	self = PyObject_New(GroupId, &GroupId_Type);
	if (!self)
		return NULL;
	self->gid = gid;
	return (PyObject *)self;
}

static void
group_id_dealloc(GroupId *v)
{
	PyObject_Del(v);
}

static PyObject *
group_id_repr(GroupId *v)
{
	char buf[80];
	sprintf(buf, "<group_id %08X:%08X:%08X>",
		v->gid.id[0], v->gid.id[1], v->gid.id[2]);
	return PyUnicode_FromString(buf);
}

static PyObject *
group_id_richcompare(PyObject *v, PyObject *w, int op)
{
	PyObject *res;

	if (!GroupId_Check(v) || !GroupId_Check(w) ||
	    (op != Py_EQ && op != Py_NE)) {
		Py_INCREF(Py_NotImplemented);
		return Py_NotImplemented;
	}

	if (SP_equal_group_ids(((GroupId *)v)->gid, ((GroupId *)w)->gid) ==
	    (op == Py_NE))
		res = Py_False;
	else
		res = Py_True;
	Py_INCREF(res);
	return res;
}

static PyTypeObject GroupId_Type = {
	/* The ob_type field must be initialized in the module init function
	 * to be portable to Windows without using C++. */
	PyVarObject_HEAD_INIT(NULL, 0)
	"GroupId",				/* tp_name */
	sizeof(GroupId),			/* tp_basicsize */
	0,					/* tp_itemsize */
	/* methods */
	(destructor)group_id_dealloc,		/* tp_dealloc */
	0,					/* tp_print */
	0,					/* tp_getattr */
	0,					/* tp_setattr */
	0,					/* tp_compare */
	(reprfunc)group_id_repr,		/* tp_repr */
	0,					/* tp_as_number */
	0,					/* tp_as_sequence */
	0,					/* tp_as_mapping */
	0,					/* tp_hash */
	0,					/* tp_call */
	0,					/* tp_str */
	0,					/* tp_getattro */
	0,					/* tp_setattro */
	0,					/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT,			/* tp_flags */
	0,					/* tp_doc */
	0,					/* tp_traverse */
	0,					/* tp_clear */
	group_id_richcompare,			/* tp_richcompare */
};

#define CAUSED_BY_MASK (CAUSED_BY_JOIN | CAUSED_BY_LEAVE | \
                        CAUSED_BY_DISCONNECT | CAUSED_BY_NETWORK)

static PyObject *
new_membership_msg(int type, PyObject *group, int num_members,
		   char (*members)[MAX_GROUP_NAME], char *buffer, int size)
{
    MembershipMsg *self;
    group_id grp_id;
    membership_info memb_info;
    int32 num_extra_members = 0;
    int i;
    int ret;

    assert(group != NULL);
    self = PyObject_New(MembershipMsg, &MembershipMsg_Type);
    if (self == NULL)
        return NULL;
    self->group = NULL;
    self->group_id = NULL;
    self->members = NULL;
    self->extra = NULL;
    self->changed_member = NULL;
    self->reason = type & CAUSED_BY_MASK; /* from sp.h defines */
    self->msg_subtype = type & (TRANSITION_MESS | REG_MEMB_MESS);
    Py_INCREF(group);
    self->group = group;
    self->members = PyTuple_New(num_members);
    if (self->members == NULL) {
        Py_DECREF(self);
        return NULL;
    }
    for (i = 0; i < num_members; ++i) {
        PyObject *s = PyUnicode_FromString(members[i]);
        if (!s) {
            Py_DECREF(self);
            return NULL;
        }
        PyTuple_SET_ITEM(self->members, i, s);
    }


    if ((ret = SP_get_memb_info(buffer, type, &memb_info)) < 0) {
	      PyErr_Format(SpreadError, "error %d on SP_get_memb_info", ret);
        Py_DECREF(self);
        return NULL;
    }


    memcpy(&grp_id, &memb_info.gid, sizeof(group_id));
    self->group_id = new_group_id(grp_id);
    if (self->group_id == NULL) {
        Py_DECREF(self);
        return NULL;
    }


    /* The extra attribute is a tuple initialized for 0 or more items.
     * If the member event is a single member event such as a join, leave or disconnect,
     * the changed member attribute is set from memb_info.changed_member
     * and a single value from vs_set, which is stored inside the message, is added to extra tuple.
     * If the member event is a merge or partition, the list of member names
     * from vs_set is stored in extra.
     */


    if (Is_reg_memb_mess(type) && (Is_caused_join_mess(type) || Is_caused_disconnect_mess(type) || Is_caused_leave_mess(type))) {
        self->changed_member =  PyUnicode_FromString(memb_info.changed_member);
        if (!self->changed_member) {
            Py_DECREF(self);
            return NULL;
        }
    }
    else {
        self->changed_member = Py_BuildValue("");
    }

    num_extra_members = memb_info.my_vs_set.num_members;

    self->extra = PyTuple_New(num_extra_members);
    if (self->extra == NULL) {
        Py_DECREF(self);
        return NULL;
    }

    if (num_extra_members > 0) {

        char (*member_names)[MAX_GROUP_NAME] = (char (*)[MAX_GROUP_NAME]) malloc(num_extra_members * MAX_GROUP_NAME);

        if (member_names == NULL) {
            Py_DECREF(self);
            PyErr_NoMemory();
            return NULL;
        }

        if ((ret = SP_get_vs_set_members(buffer, &memb_info.my_vs_set, member_names, num_extra_members)) < 0) {
	          PyErr_Format(SpreadError, "error %d on SP_get_vs_set_members", ret);
            Py_DECREF(self);
            free(member_names);
            return NULL;
        }

        for (i = 0; i < num_extra_members; i++) {
            PyObject *s;
            s = PyUnicode_FromString(member_names[i]);
            if (!s) {
                Py_DECREF(self);
                free(member_names);
                return NULL;
            }
            PyTuple_SET_ITEM(self->extra, i, s);
        }

        free(member_names);
    }

    return (PyObject *)self;
}

static void
membership_msg_dealloc(MembershipMsg *self)
{
	Py_XDECREF(self->group);
	Py_XDECREF(self->members);
	Py_XDECREF(self->extra);
	Py_XDECREF(self->group_id);
  Py_XDECREF(self->changed_member);
	PyObject_Del(self);
}

#undef OFF

static PyObject *
membership_msg_getattr(MembershipMsg *self, char *name)
{
    PyObject *res = NULL;
    if (strcmp(name, "reason") == 0) {
        res = PyLong_FromLong(self->reason);
    } else if (strcmp(name, "msg_subtype") == 0) {
        res = PyLong_FromLong(self->msg_subtype);
    } else if (strcmp(name, "group") == 0) {
        res = self->group;
    } else if (strcmp(name, "group_id") == 0) {
        res = self->group_id;
    } else if (strcmp(name, "members") == 0) {
        res = self->members;
    } else if (strcmp(name, "extra") == 0) {
        res = self->extra;
    } else if (strcmp(name, "changed_member") == 0) {
        res = self->changed_member;
    } else {
        PyErr_SetString(PyExc_AttributeError, name);
        return NULL;
    }
    Py_XINCREF(res);
    return res;
}

static PyTypeObject MembershipMsg_Type = {
	/* The ob_type field must be initialized in the module init function
	 * to be portable to Windows without using C++. */
	PyVarObject_HEAD_INIT(NULL, 0)
	"MembershipMsg",			/* tp_name */
	sizeof(MembershipMsg),			/* tp_basicsize */
	0,					/* tp_itemsize */
	/* methods */
	(destructor)membership_msg_dealloc,	/* tp_dealloc */
	0,					/* tp_print */
	(getattrfunc)membership_msg_getattr,	/* tp_getattr */
	0,					/* tp_setattr */
	0,					/* tp_compare */
	0,					/* tp_repr */
	0,					/* tp_as_number */
	0,					/* tp_as_sequence */
	0,					/* tp_as_mapping */
	0,					/* tp_hash */
	0,					/* tp_call */
	0,					/* tp_str */
	0,					/* tp_getattro */
	0,					/* tp_setattro */
	0,					/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT,			/* tp_flags */
	0,					/* tp_doc */
	0,					/* tp_traverse */
	0,					/* tp_clear */
	0,					/* tp_richcompare */
};

static PyObject *
new_regular_msg(PyObject *sender, int num_groups,
		char (*groups)[MAX_GROUP_NAME], int msg_type,
		int endian, PyObject *message)
{
	RegularMsg *self;
	int i;

	self = PyObject_New(RegularMsg, &RegularMsg_Type);
	if (self == NULL)
		return NULL;
	self->sender = NULL;
	self->groups = NULL;
	self->message = NULL;
	assert(num_groups >= 0);
	self->groups = PyTuple_New(num_groups);
	if (self->groups == NULL) {
		Py_DECREF(self);
		return NULL;
	}
	for (i = 0; i < num_groups; ++i) {
		PyObject *s = PyUnicode_FromString(groups[i]);
		if (!s) {
			Py_DECREF(self);
			return NULL;
		}
		PyTuple_SET_ITEM(self->groups, i, s);
	}

	Py_INCREF(sender);
	self->sender = sender;
	Py_INCREF(message);
	self->message = message;
	self->msg_type = msg_type;
	self->endian = endian;
	return (PyObject *)self;
}

static void
regular_msg_dealloc(RegularMsg *self)
{
	Py_XDECREF(self->sender);
	Py_XDECREF(self->groups);
	Py_XDECREF(self->message);
	PyObject_Del(self);
}

#define OFF(x) offsetof(RegularMsg, x)

static struct PyMemberDef RegularMsg_memberlist[] = {
	{"msg_type", T_INT,		OFF(msg_type), READONLY},
	{"endian",   T_INT,		OFF(endian), READONLY},
	{"sender",   T_OBJECT_EX,		OFF(sender), READONLY},
	{"groups",   T_OBJECT_EX,		OFF(groups), READONLY},
	{"message",  T_OBJECT_EX,		OFF(message), READONLY},
	{NULL}
};

#undef OFF

static PyObject *
regular_msg_getattr(RegularMsg *self, char *name)
{
    PyObject *res = NULL;
    if (strcmp(name, "msg_type") == 0) {
        res = PyLong_FromLong(self->msg_type);
    } else if (strcmp(name, "endian") == 0) {
        res = PyLong_FromLong(self->endian);
    } else if (strcmp(name, "sender") == 0) {
        res = self->sender;
    } else if (strcmp(name, "groups") == 0) {
        res = self->groups;
    } else if (strcmp(name, "message") == 0) {
        res = self->message;
    } else {
        PyErr_SetString(PyExc_AttributeError, name);
        return NULL;
    }
    Py_XINCREF(res);
    return res;
}

static PyTypeObject RegularMsg_Type = {
	/* The ob_type field must be initialized in the module init function
	 * to be portable to Windows without using C++. */
	PyVarObject_HEAD_INIT(NULL, 0)
	"RegularMsg",				/* tp_name */
	sizeof(RegularMsg),			/* tp_basicsize */
	0,					/* tp_itemsize */
	/* methods */
	(destructor)regular_msg_dealloc,	/* tp_dealloc */
	0,					/* tp_print */
	(getattrfunc)regular_msg_getattr,	/* tp_getattr */
	0,					/* tp_setattr */
	0,					/* tp_compare */
	0,					/* tp_repr */
	0,					/* tp_as_number */
	0,					/* tp_as_sequence */
	0,					/* tp_as_mapping */
	0,					/* tp_hash */
	0,					/* tp_call */
	0,					/* tp_str */
	0,					/* tp_getattro */
	0,					/* tp_setattro */
	0,					/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT,			/* tp_flags */
	0,					/* tp_doc */
	0,					/* tp_traverse */
	0,					/* tp_clear */
	0,					/* tp_richcompare */
};

static MailboxObject *
new_mailbox(mailbox mbox)
{
	MailboxObject *self;
	self = PyObject_New(MailboxObject, &Mailbox_Type);
	if (self == NULL)
		return NULL;
	self->mbox = mbox;
	self->private_group = NULL;
	self->disconnected = 0;
#ifdef SPREAD_DISCONNECT_RACE_BUG
	self->spread_lock = NULL;
#endif
	return self;
}

/* mailbox methods */

static void
mailbox_dealloc(MailboxObject *self)
{
	if (self->disconnected == 0)
		SP_disconnect(self->mbox);
	Py_DECREF(self->private_group);
#ifdef SPREAD_DISCONNECT_RACE_BUG
	if (self->spread_lock)
		PyThread_free_lock(self->spread_lock);
#endif
	PyObject_Del(self);
}

static PyObject *
err_disconnected(char *methodname)
{
	PyErr_Format(SpreadError, "%s() called on closed mbox", methodname);
	return NULL;
}

static PyObject *
mailbox_disconnect(MailboxObject *self, PyObject *args)
{
	PyObject *result = Py_None;

	if (!PyArg_ParseTuple(args, ":disconnect"))
		return NULL;
	if (!self->disconnected) {
		ACQUIRE_MBOX_LOCK(self);
		if (!self->disconnected) {
			int err;
			self->disconnected = 1;
			Py_BEGIN_ALLOW_THREADS
			err = SP_disconnect(self->mbox);
			Py_END_ALLOW_THREADS
			if (err != 0)
				result = spread_error(err, self);
		}
		RELEASE_MBOX_LOCK(self);
	}
	Py_XINCREF(result);
	return result;
}

static PyObject *
mailbox_fileno(MailboxObject *self, PyObject *args)
{
	if (!PyArg_ParseTuple(args, ":fileno"))
		return NULL;
	if (self->disconnected)
		return err_disconnected("fileno");
	return PyLong_FromLong(self->mbox);
}

static PyObject *
mailbox_join(MailboxObject *self, PyObject *args)
{
	char *group;
	PyObject *result = Py_None;

	if (!PyArg_ParseTuple(args, "s:join", &group))
		return NULL;
	ACQUIRE_MBOX_LOCK(self);
	if (self->disconnected)
		result = err_disconnected("join");
	else {
		int err;
		Py_BEGIN_ALLOW_THREADS
		err = SP_join(self->mbox, group);
		Py_END_ALLOW_THREADS
		if (err < 0)
			result = spread_error(err, self);
	}
	RELEASE_MBOX_LOCK(self);
	Py_XINCREF(result);
	return result;
}

static PyObject *
mailbox_leave(MailboxObject *self, PyObject *args)
{
	char *group;
	PyObject *result = Py_None;

	if (!PyArg_ParseTuple(args, "s:leave", &group))
		return NULL;
	ACQUIRE_MBOX_LOCK(self);
	if (self->disconnected)
		result = err_disconnected("leave");
	else {
		int err;
		Py_BEGIN_ALLOW_THREADS
		err = SP_leave(self->mbox, group);
		Py_END_ALLOW_THREADS
		if (err < 0)
			result = spread_error(err, self);
	}
	RELEASE_MBOX_LOCK(self);
	Py_XINCREF(result);
	return result;
}

static PyObject *
mailbox_receive(MailboxObject *self, PyObject *args)
{
    service svc_type;
    int num_groups = 0, endian = 0, size = 0;
    int16 msg_type;
    char senderbuffer[MAX_GROUP_NAME];
    char groupbuffer[DEFAULT_GROUPS_SIZE][MAX_GROUP_NAME];
    int max_groups = DEFAULT_GROUPS_SIZE;
    char (*groups)[MAX_GROUP_NAME] = groupbuffer;
    PyObject *data_byte_array = PyByteArray_FromObject(PyBytes_FromStringAndSize(NULL, DEFAULT_BUFFER_SIZE));
    if (data_byte_array == NULL) {
        return NULL;
    }
    char* pbuffer = PyByteArray_AsString(data_byte_array);
    int bufsize = DEFAULT_BUFFER_SIZE;

    if (!PyArg_ParseTuple(args, ":receive"))
        return NULL;

    ACQUIRE_MBOX_LOCK(self);
    if (self->disconnected) {
        err_disconnected("receive");
        goto error;
    }

    for (;;) {
        Py_BEGIN_ALLOW_THREADS
        svc_type = 0;
        size = SP_receive(self->mbox, &svc_type, senderbuffer, max_groups, &num_groups, groups, &msg_type, &endian, bufsize, pbuffer);
        Py_END_ALLOW_THREADS

        if (size >= 0) {
            break;
        }
        if (size == BUFFER_TOO_SHORT) {
            bufsize = -endian;
            if (PyByteArray_Resize(data_byte_array, bufsize) < 0) {
                goto error;
            }
            pbuffer = PyByteArray_AsString(data_byte_array);
            continue;
        }
        if (size == GROUPS_TOO_SHORT) {
            max_groups = -num_groups;
            if (groups != groupbuffer)
                free(groups);
            groups = malloc(MAX_GROUP_NAME * max_groups);
            if (groups == NULL) {
                PyErr_NoMemory();
                goto error;
            }
            continue;
        }
        spread_error(size, self);
        goto error;
    }

    PyObject* result = NULL;
    PyObject* sender_obj = NULL;
    PyObject* group_obj = NULL;
    PyObject* message_obj = NULL;

    if (Is_regular_mess(svc_type)) {
        sender_obj = PyUnicode_FromString(senderbuffer);
        if (sender_obj == NULL) {
            goto error;
        }
        message_obj = PyBytes_FromStringAndSize(pbuffer, size);
        if (message_obj == NULL) {
            Py_DECREF(sender_obj);
            goto error;
        }
        result = new_regular_msg(sender_obj, num_groups, groups, msg_type, endian, message_obj);
        Py_DECREF(sender_obj);
        Py_DECREF(message_obj);
    } else if (Is_membership_mess(svc_type)) {
        result = new_membership_msg(svc_type, PyUnicode_FromString(senderbuffer), num_groups, groups, pbuffer, size);
    } else {
        PyErr_Format(SpreadError, "Unknown message type: %d", svc_type);
        goto error;
    }

    RELEASE_MBOX_LOCK(self);
    if (groups != groupbuffer)
        free(groups);
    Py_XDECREF(data_byte_array);
    return result;

error:
    RELEASE_MBOX_LOCK(self);
    if (groups != groupbuffer)
        free(groups);
    Py_XDECREF(data_byte_array);
    return NULL;
}

const int valid_svc_type = (UNRELIABLE_MESS | RELIABLE_MESS | FIFO_MESS
			    | CAUSAL_MESS | AGREED_MESS | SAFE_MESS
			    | SELF_DISCARD);

static PyObject *
mailbox_multicast(MailboxObject *self, PyObject *args)
{
	int svc_type, bytes, msg_len;
	int msg_type = 0;
	char *group, *msg;
	PyObject *result = NULL;

	if (!PyArg_ParseTuple(args, "iss#|i:multicast",
			      &svc_type, &group, &msg, &msg_len, &msg_type))
		return NULL;

	ACQUIRE_MBOX_LOCK(self);
	if (self->disconnected) {
		err_disconnected("multicast");
		goto Done;
	}
	/* XXX This doesn't check that svc_type is set to exactly one of
	   the service types. */
	if ((svc_type & valid_svc_type) != svc_type) {
		PyErr_SetString(PyExc_ValueError, "invalid service type");
		goto Done;;
	}

	Py_BEGIN_ALLOW_THREADS
	bytes = SP_multicast(self->mbox, svc_type, group, (int16)msg_type,
			     msg_len, msg);
	Py_END_ALLOW_THREADS
	if (bytes < 0)
		result = spread_error(bytes, self);
	else
		result = PyLong_FromLong(bytes);
Done:
	RELEASE_MBOX_LOCK(self);
	return result;
}

static PyObject *
mailbox_multigroup_multicast(MailboxObject *self, PyObject *args)
{
        int svc_type, bytes, msg_len, group_len;
        int msg_type = 0;
	PyObject *group_tuple, *temp;
        char *msg;

	char (*groups)[MAX_GROUP_NAME];
	int index;

        PyObject *result = NULL;

        if (! PyArg_ParseTuple(args, "iO!s#|i:multicast",
                               &svc_type,
                               &PyTuple_Type, &group_tuple,
                               &msg, &msg_len,
                               &msg_type))
                return NULL;

	if(! PyTuple_Check(group_tuple)) {
		PyErr_SetString(PyExc_TypeError,
				"only tuples are allowed for groups");
		return NULL;
	}

	group_len = PyTuple_Size(group_tuple);
	if (group_len == 0) {
		PyErr_SetString(PyExc_ValueError,
				"there must be at least one group in the tuple");
		return NULL;
	}

	groups = malloc(MAX_GROUP_NAME * group_len);
	if (groups == NULL) {
		PyErr_NoMemory();
		return NULL;
	}

	for (index = 0; index < group_len; index++) {
		temp = PyTuple_GetItem(group_tuple, index);
		if(! PyUnicode_Check(temp)) {
			PyErr_SetString(PyExc_TypeError,
					"groups must be strings only");
			goto Done;
		}
		strncpy(groups[index],
			PyUnicode_AsUTF8(PyTuple_GetItem(group_tuple, index)),
			MAX_GROUP_NAME);
	}

	ACQUIRE_MBOX_LOCK(self);
	if (self->disconnected) {
		err_disconnected("multigroup_multicast");
		goto Done;
	}

	/* XXX This doesn't check that svc_type is set to exactly one of
	   the service types. */
	if ((svc_type & valid_svc_type) != svc_type) {
		PyErr_SetString(PyExc_ValueError, "invalid service type");
		goto Done;
	}

	Py_BEGIN_ALLOW_THREADS
	bytes = SP_multigroup_multicast(self->mbox, svc_type, group_len,
				        (const char (*)[MAX_GROUP_NAME]) groups,
		                        (int16)msg_type, msg_len, msg);
	Py_END_ALLOW_THREADS

	if (bytes < 0)
		result = spread_error(bytes, self);
	else
		result = PyLong_FromLong(bytes);

Done:
	RELEASE_MBOX_LOCK(self);
	free(groups);
	return result;
}

static PyObject *
mailbox_poll(MailboxObject *self, PyObject *args)
{
	int bytes;
	PyObject *result = NULL;

	if (!PyArg_ParseTuple(args, ":poll"))
		return NULL;
	ACQUIRE_MBOX_LOCK(self);
	if (self->disconnected) {
		err_disconnected("poll");
		goto Done;
	}
	Py_BEGIN_ALLOW_THREADS
	bytes = SP_poll(self->mbox);
	Py_END_ALLOW_THREADS
	if (bytes < 0)
		result = spread_error(bytes, self);
	else
		result = PyLong_FromLong(bytes);
Done:
	RELEASE_MBOX_LOCK(self);
	return result;
}

static PyMethodDef Mailbox_methods[] = {
	{"disconnect",	(PyCFunction)mailbox_disconnect,METH_VARARGS},
	{"fileno",	(PyCFunction)mailbox_fileno,	METH_VARARGS},
	{"join",	(PyCFunction)mailbox_join,	METH_VARARGS},
	{"leave",	(PyCFunction)mailbox_leave,	METH_VARARGS},
	{"multicast",   (PyCFunction)mailbox_multicast, METH_VARARGS},
	{"multigroup_multicast",	(PyCFunction)mailbox_multigroup_multicast, METH_VARARGS},
	{"poll",	(PyCFunction)mailbox_poll,	METH_VARARGS},
	{"receive",	(PyCFunction)mailbox_receive,	METH_VARARGS},
	{NULL,		NULL}		/* sentinel */
};

#define OFF(x) offsetof(MailboxObject, x)

static struct PyMemberDef Mailbox_memberlist[] = {
	{"private_group",	T_OBJECT_EX,	OFF(private_group), READONLY},
	{NULL}
};

static PyObject *
mailbox_getattr(PyObject *self, char *name)
{
    PyMethodDef *p;

    for (p = Mailbox_methods; p->ml_name != NULL; p++) {
        if (strcmp(name, p->ml_name) == 0) {
            return PyCFunction_New(p, self);
        }
    }
	PyErr_Clear();
	return PyMember_GetOne((char *)self, Mailbox_memberlist);
}

static PyTypeObject Mailbox_Type = {
	/* The ob_type field must be initialized in the module init function
	 * to be portable to Windows without using C++. */
	PyVarObject_HEAD_INIT(NULL, 0)
	"Mailbox",				/* tp_name */
	sizeof(MailboxObject),			/* tp_basicsize */
	0,					/* tp_itemsize */
	/* methods */
	(destructor)mailbox_dealloc,		/* tp_dealloc */
	0,					/* tp_print */
	(getattrfunc)mailbox_getattr,		/* tp_getattr */
	0,					/* tp_setattr */
	0,					/* tp_compare */
	0,					/* tp_repr */
	0,					/* tp_as_number */
	0,					/* tp_as_sequence */
	0,					/* tp_as_mapping */
};

static char spread_connect__doc__[] =
"connect(daemon=\"N@localhost\", name=\"\", priority=0, membership=1) -> mbox\n"
"\n"
"All arguments are optional, and can be specified by keyword or position.\n"
"\n"
"Connect to a Spread daemon, via Spread's SP_connect().  Return a Mailbox\n"
"object representing the connection.  Communication with Spread on this\n"
"connection is done via invoking methods of the Mailbox object.\n"
"\n"
"'daemon' is the name of the desired Spread daemon.  It defaults to\n"
"    \"%d@localhost\" % spread.DEFAULT_SPREAD_PORT\n"
"'name' is the desired private name for the connection.  It defaults to an\n"
"    empty string, in which case Spread generates a unique random name.\n"
"'priority' is an int, default 0, currently unused (see Spread docs).\n"
"'membership' is a Boolean, default 1 (true), determining whether you want\n"
"    to receive membership messages on this connection.  If your application\n"
"    doesn't make mbox.receive() calls, pass 0 to avoid creating an\n"
"    unboundedly large queue of unread membership messages.\n"
"\n"
"Upon successful connect, mbox.private_group is the private group name\n"
"Spread assigned to the connection.";

static PyObject *
spread_connect(PyObject *self, PyObject *args, PyObject* kwds)
{
	static char *kwlist[] = {"daemon", "name", "priority", "membership",
				  0};
	char *daemon = NULL;
	char *name = "";
	int priority = 0;
	int membership = 1;

	MailboxObject *mbox;
	mailbox _mbox;
	int ret;
	PyObject *group_name = NULL;
	char default_daemon[100];

	if (!PyArg_ParseTupleAndKeywords(args, kwds, "|ssii:connect", kwlist,
					 &daemon, &name, &priority,
					 &membership))
		return NULL;

	if (daemon == NULL) {
		/* XXX Can't use PyOS_snprintf before 2.2.
		PyOS_snprintf(default_daemon, sizeof(default_daemon),
			      "%d@localhost", DEFAULT_SPREAD_PORT);
		*/
		sprintf(default_daemon, "%d@localhost", DEFAULT_SPREAD_PORT);
		daemon = default_daemon;
	}

	/* initialize output buffer for group name */
	group_name = PyUnicode_New(MAX_GROUP_NAME, 0);
	if (group_name == NULL)
		return NULL;

	Py_BEGIN_ALLOW_THREADS
	ret = SP_connect(daemon, name, priority, membership, &_mbox,
			 (char *)PyUnicode_AsUTF8(group_name));
	Py_END_ALLOW_THREADS

	if (ret != ACCEPT_SESSION) {
		Py_DECREF(group_name);
		return spread_error(ret, NULL);
	}

	mbox = new_mailbox(_mbox);
	if (mbox == NULL) {
		SP_disconnect(_mbox);
		Py_DECREF(group_name);
		return NULL;
	}
#ifdef SPREAD_DISCONNECT_RACE_BUG
	mbox->spread_lock = PyThread_allocate_lock();
	if (mbox->spread_lock == NULL) {
		Py_DECREF(mbox);
		return NULL;
	}
#endif
	if (PyUnicode_Resize(
		&group_name,
		strlen(PyUnicode_AsUTF8(group_name))) < 0)
	{
		SP_disconnect(_mbox);
		Py_DECREF(mbox);
		return NULL;
	}
	mbox->private_group = group_name;
	return (PyObject*)mbox;
}

static char spread_version__doc__[] =
"version() -> (major, minor, patch)\n"
"\n"
"Return Spread's version number as a 3-tuple of integers, as obtained\n"
"from Spread's SP_version().";

static PyObject *
spread_version(PyObject *self, PyObject *args)
{
	int major, minor, patch;

	if (!PyArg_ParseTuple(args, ":version"))
		return NULL;
	if (!SP_version(&major, &minor, &patch)) {
		PyErr_SetString(SpreadError, "SP_version failed");
		return NULL;
	}
	return Py_BuildValue("iii", major, minor, patch);
}

/* List of functions defined in the module */

static PyMethodDef spread_methods[] = {
	{"connect", (PyCFunction)spread_connect, METH_VARARGS | METH_KEYWORDS,
	 spread_connect__doc__},
	{"version", spread_version, METH_VARARGS,
	 spread_version__doc__},
	{NULL, NULL}		/* sentinel */
};

/* spread_error(): helper function for setting exceptions from SP_xxx
   return value */

static PyObject *
spread_error(int err, MailboxObject *mbox)
{
	char *message = NULL;
	PyObject *val;

	/* XXX It would be better if spread provided an API function to
	   map these to error strings.  SP_error() merely prints a string,
	   which is useful in only limited circumstances. */
	switch (err) {
	case ILLEGAL_SPREAD:
		message = "Illegal spread was provided";
		break;
	case COULD_NOT_CONNECT:
		message = "Could not connect. Is Spread running?";
		break;
	case REJECT_QUOTA:
		message = "Connection rejected, too many users";
		break;
	case REJECT_NO_NAME:
		message = "Connection rejected, no name was supplied";
		break;
	case REJECT_ILLEGAL_NAME:
		message = "Connection rejected, illegal name";
		break;
	case REJECT_NOT_UNIQUE:
		message = "Connection rejected, name not unique";
		break;
	case REJECT_VERSION:
		message = "Connection rejected, library does not fit daemon";
		break;
	case CONNECTION_CLOSED:
		message = "Connection closed by spread";
		if (mbox)
			mbox->disconnected = 1;
		break;
	case REJECT_AUTH:
		message = "Connection rejected, authentication failed";
		break;
	case ILLEGAL_SESSION:
		message = "Illegal session was supplied";
		if (mbox)
			mbox->disconnected = 1;
		break;
	case ILLEGAL_SERVICE:
		message = "Illegal service request";
		break;
	case ILLEGAL_MESSAGE:
		message = "Illegal message";
		break;
	case ILLEGAL_GROUP:
		message = "Illegal group";
		break;
	case BUFFER_TOO_SHORT:
		message = "The supplied buffer was too short";
		break;
	case GROUPS_TOO_SHORT:
		message = "The supplied groups list was too short";
		break;
	case MESSAGE_TOO_LONG:
		message = "The message body + group names "
			  "was too large to fit in a message";
		break;
	default:
		message = "unrecognized error";
	}

	val = Py_BuildValue("is", err, message);
	if (val) {
		PyErr_SetObject(SpreadError, val);
		Py_DECREF(val);
	}

	return NULL;
}

/* Table of symbolic constants defined by Spread.
   (Programmatically generated from sp.h.) */

static struct constdef {
	char *name;
	int value;
} spread_constants[] = {
	{"LOW_PRIORITY", LOW_PRIORITY},
	{"MEDIUM_PRIORITY", MEDIUM_PRIORITY},
	{"HIGH_PRIORITY", HIGH_PRIORITY},
	{"DEFAULT_SPREAD_PORT", DEFAULT_SPREAD_PORT},
	{"SPREAD_VERSION", SPREAD_VERSION},
	{"MAX_GROUP_NAME", MAX_GROUP_NAME},
	{"MAX_PRIVATE_NAME", MAX_PRIVATE_NAME},
	{"MAX_PROC_NAME", MAX_PROC_NAME},
	{"UNRELIABLE_MESS", UNRELIABLE_MESS},
	{"RELIABLE_MESS", RELIABLE_MESS},
	{"FIFO_MESS", FIFO_MESS},
	{"CAUSAL_MESS", CAUSAL_MESS},
	{"AGREED_MESS", AGREED_MESS},
	{"SAFE_MESS", SAFE_MESS},
	{"REGULAR_MESS", REGULAR_MESS},
	{"SELF_DISCARD", SELF_DISCARD},
	{"DROP_RECV", DROP_RECV},
	{"REG_MEMB_MESS", REG_MEMB_MESS},
	{"TRANSITION_MESS", TRANSITION_MESS},
	{"CAUSED_BY_JOIN", CAUSED_BY_JOIN},
	{"CAUSED_BY_LEAVE", CAUSED_BY_LEAVE},
	{"CAUSED_BY_DISCONNECT", CAUSED_BY_DISCONNECT},
	{"CAUSED_BY_NETWORK", CAUSED_BY_NETWORK},
	{"MEMBERSHIP_MESS", MEMBERSHIP_MESS},
	{"ENDIAN_RESERVED", ENDIAN_RESERVED},
	{"RESERVED", RESERVED},
	{"REJECT_MESS", REJECT_MESS},
	{"ACCEPT_SESSION", ACCEPT_SESSION},
	{"ILLEGAL_SPREAD", ILLEGAL_SPREAD},
	{"COULD_NOT_CONNECT", COULD_NOT_CONNECT},
	{"REJECT_QUOTA", REJECT_QUOTA},
	{"REJECT_NO_NAME", REJECT_NO_NAME},
	{"REJECT_ILLEGAL_NAME", REJECT_ILLEGAL_NAME},
	{"REJECT_NOT_UNIQUE", REJECT_NOT_UNIQUE},
	{"REJECT_VERSION", REJECT_VERSION},
	{"CONNECTION_CLOSED", CONNECTION_CLOSED},
	{"REJECT_AUTH", REJECT_AUTH},
	{"ILLEGAL_SESSION", ILLEGAL_SESSION},
	{"ILLEGAL_SERVICE", ILLEGAL_SERVICE},
	{"ILLEGAL_MESSAGE", ILLEGAL_MESSAGE},
	{"ILLEGAL_GROUP", ILLEGAL_GROUP},
	{"BUFFER_TOO_SHORT", BUFFER_TOO_SHORT},
	{"GROUPS_TOO_SHORT", GROUPS_TOO_SHORT},
	{"MESSAGE_TOO_LONG", MESSAGE_TOO_LONG},

	/* Not Spread constants, but still useful */
	{"DEFAULT_BUFFER_SIZE", DEFAULT_BUFFER_SIZE},
	{"DEFAULT_GROUPS_SIZE", DEFAULT_GROUPS_SIZE},
	{NULL}
};

static struct PyModuleDef spreadmodule = {
    PyModuleDef_HEAD_INIT,
    "spread",
    NULL,
    -1,
    spread_methods
};

/* Initialization function for the module */

PyMODINIT_FUNC
PyInit_spread(void)
{
	PyObject *m;
	struct constdef *p;

	/* Create the module and add the functions */
	m = PyModule_Create(&spreadmodule);
	if (m == NULL)
		return NULL;

	/* Initialize the type of the new type object here; doing it here
	 * is required for portability to Windows without requiring C++. */
	if (PyType_Ready(&Mailbox_Type) < 0) {
        return NULL;
    }
    if (PyType_Ready(&RegularMsg_Type) < 0) {
        return NULL;
    }
    if (PyType_Ready(&MembershipMsg_Type) < 0) {
        return NULL;
    }

	/* PyModule_AddObject() DECREFs its third argument */
	Py_INCREF(&Mailbox_Type);
	if (PyModule_AddObject(m, "MailboxType",
			       (PyObject *)&Mailbox_Type) < 0) {
		return NULL;
    }
	Py_INCREF(&RegularMsg_Type);
	if (PyModule_AddObject(m, "RegularMsgType",
			       (PyObject *)&RegularMsg_Type) < 0) {
		return NULL;
    }
	Py_INCREF(&MembershipMsg_Type);
	if (PyModule_AddObject(m, "MembershipMsgType",
			       (PyObject *)&MembershipMsg_Type) < 0) {
		return NULL;
    }

	/* Create the exception, if necessary */
	if (SpreadError == NULL) {
		SpreadError = PyErr_NewException("spread.error", NULL, NULL);
		if (SpreadError == NULL)
			return NULL;
	}

	/* Add the exception to the module */
	Py_INCREF(SpreadError);
	if (PyModule_AddObject(m, "error", SpreadError) < 0) {
		return NULL;
    }

	/* Add the Spread symbolic constants to the module */
	for (p = spread_constants; p->name != NULL; p++) {
		if (PyModule_AddIntConstant(m, p->name, p->value) < 0)
			return NULL;
	}

    return m;
}
