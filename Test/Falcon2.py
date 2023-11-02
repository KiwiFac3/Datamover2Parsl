import parsl
from parsl import python_app, File
from parsl.config import Config
# from parsl.data_provider.data_manager import NoOpFileStaging, FTPSeparateTaskStaging, HTTPSeparateTaskStaging
from parsl.executors import HighThroughputExecutor
import time

import sys

sys.path.insert(0, '/data/mabughosh/Datamover2Parsl/')
from data_provider.falcon2 import FalconStaging

# set the working directory and host for the receiver
root_dir = '/data/mabughosh/files/'

#134.197.94.245
#134.197.113.71
# define the conversion function
@python_app
def convert(inputs=[]):
    return inputs


# set up Parsl config
config = Config(
    executors=[
        HighThroughputExecutor(
            working_dir=root_dir,
            storage_access=[FalconStaging('134.197.113.70')],
            max_workers=8
        ),
    ],
)

# load the Parsl config
parsl.load(config)

# start a timer to record the elapsed time
start_time = time.time()

inputs = []
inputs.append(File('falcon://134.197.113.71/data/mabughosh/files/?8080'))
inputs.append(File('falcon://134.197.113.71/data/mabughosh/files1/?5000'))

#print('Here')
convert_tasks = []

# convert the input files and save the outputs
for name in inputs:
    task = convert(name)
    convert_tasks.append(task)

results = [task.result() for task in convert_tasks]
# r = convert(inputs=[file1])
# print(r.result())

# stop the timer and print the elapsed time
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Code ran in {elapsed_time:.2f} seconds")

