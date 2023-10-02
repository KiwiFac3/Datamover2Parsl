import parsl
from parsl import python_app, File
from parsl.config import Config
# from parsl.data_provider.data_manager import NoOpFileStaging, FTPSeparateTaskStaging, HTTPSeparateTaskStaging
from parsl.executors import HighThroughputExecutor
import time

import sys

sys.path.insert(0, '/data/mabughosh/Datamover2Parsl/')
from data_provider.falcon import FalconStaging


# define the conversion function
@python_app
def convert(inputs=[]):
    return inputs


# set up Parsl config
config = Config(
    executors=[
        HighThroughputExecutor(
            storage_access=[FalconStaging()],
            max_workers=8
        ),
    ],
)

# load the Parsl config
parsl.load(config)

# start a timer to record the elapsed time
start_time = time.time()

file1 = File('falcon://134.197.113.71/data/mabughosh/files/')

r = convert(inputs=[file1])
print(r.result())

# stop the timer and print the elapsed time
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Code ran in {elapsed_time:.2f} seconds")
