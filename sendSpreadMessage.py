#!/usr/bin/env python3
import argparse
import sys
import os
import time

# Add the build directory to the path to find the spread module
def setup_path():
    for root, dirs, files in os.walk('build'):
        for name in files:
            if name.startswith('spread') and name.endswith('.so'):
                sys.path.insert(0, root)
                return

setup_path()

try:
    import spread
except ImportError:
    print("Error: The 'spread' module could not be imported. Make sure it's built and in your Python path.")
    sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Send a message via Spread.")
    parser.add_argument("--ip", default=f"{spread.DEFAULT_SPREAD_PORT}@localhost",
                        help=f"Spread daemon IP address and port (e.g., {spread.DEFAULT_SPREAD_PORT}@localhost). "
                             "Defaults to %(default)s.")
    parser.add_argument("--username", default="python_user",
                        help="Username for the Spread connection. Defaults to %(default)s.")
    parser.add_argument("--group", default="test_group",
                        help="Group to send the message to. Defaults to %(default)s.")
    parser.add_argument("--message", default="Hello from Python Spread!",
                        help="Message to send. Defaults to '%(default)s'.")

    args = parser.parse_args()

    daemon_address = args.ip
    username = args.username
    group_name = args.group
    message_content = args.message.encode('utf-8') # Messages should be bytes

    print(f"Attempting to connect to Spread daemon at {daemon_address} as user '{username}'...")
    try:
        # Connect to Spread
        # spread.connect(daemon, name, priority, membership)
        mbox = spread.connect(daemon_address, username, 0, 1)
        print(f"Successfully connected. Private group: {mbox.private_group}")

        # Join the specified group
        print(f"Joining group '{group_name}'...")
        mbox.join(group_name)
        # Wait for membership message
        while True:
            if mbox.poll():
                msg = mbox.receive()
                if isinstance(msg, spread.MembershipMsgType) and msg.group == group_name:
                    print(f"Joined group '{group_name}'. Current members: {msg.members}")
                    break
            time.sleep(0.1)

        # Send the message
        print(f"Sending message to group '{group_name}': '{args.message}'...")
        mbox.multicast(spread.SAFE_MESS, group_name, message_content)
        print("Message sent.")

        # Optionally, receive the message to confirm (if in the group)
        print("Waiting for message confirmation (if sent to self or other members)...")
        start_time = time.time()
        received = False
        while time.time() - start_time < 5: # Wait up to 5 seconds
            if mbox.poll():
                msg = mbox.receive()
                if isinstance(msg, spread.RegularMsgType) and msg.message == message_content:
                    print(f"Received confirmation: '{msg.message.decode('utf-8')}' from '{msg.sender}'")
                    received = True
                    break
            time.sleep(0.1)
        if not received:
            print("No confirmation message received within 5 seconds.")

    except spread.error as e:
        print(f"Spread Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)
    finally:
        if 'mbox' in locals() and not mbox.disconnected:
            print("Disconnecting from Spread...")
            mbox.disconnect()
            print("Disconnected.")

if __name__ == "__main__":
    main()
