import zmq
import subprocess
import json
import time

zmq_context = zmq.Context()
zmq_socket = zmq_context.socket(zmq.REP)
zmq_socket.bind("tcp://*:5555")

# Wait for the next request from the client
json_data = zmq_socket.recv_string()
print("Received JSON data:", json_data)

# Parse the JSON data
data = json.loads(json_data)

# Extract the message and host from the data
host = data.get("host", "No host provided")
port = data.get("port", "No port provided")
file_path = data.get("file_path", "No file path provided")

# Start the sender
sender_command = ["falcon", "sender", "--host", host, "--port", port, "--data_dir",
                  file_path, "--method", "probe"]
print(sender_command)
time.sleep(0.1)
try:
    subprocess.run(sender_command, check=True)
except subprocess.CalledProcessError as e:
    print(f"Command failed with exit code {e.returncode}: {e.stderr}")
except FileNotFoundError:
    print("The 'falcon' command was not found. Make sure it's installed and in your system's PATH.")

# Send the response back to the client
zmq_socket.send_string("Received")

zmq_socket.close()
zmq_context.term()


