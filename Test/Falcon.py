import parsl
from parsl import python_app, File
from parsl.config import Config
# from parsl.data_provider.data_manager import NoOpFileStaging, FTPSeparateTaskStaging, HTTPSeparateTaskStaging
from parsl.executors import HighThroughputExecutor
import time

import sys

sys.path.insert(0, '/home/mabughosh/Datamover2Parsl/')
from data_provider.falcon import FalconStaging

# set the working directory and host for the receiver
working_dir = '/home/mabughosh/Files/'


# define the conversion function
@python_app
def convert(inputs=[]):
    return inputs


# set up Parsl config
config = Config(
    executors=[
        HighThroughputExecutor(
            working_dir=working_dir,
            storage_access=[FalconStaging("134.197.94.245")],
            max_workers=8
        ),
    ],
)

# load the Parsl config
parsl.load(config)

# start a timer to record the elapsed time
start_time = time.time()

file1 = File('falcon:')

r = convert(inputs=[file1])
print(r.result())

# stop the timer and print the elapsed time
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Code ran in {elapsed_time:.2f} seconds")
