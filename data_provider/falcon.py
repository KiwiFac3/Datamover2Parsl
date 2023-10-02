import logging
from functools import partial
import subprocess
from typing import Optional

from parsl.app.app import python_app
from parsl.data_provider.staging import Staging
from parsl.utils import RepresentationMixin

# Initialize the logger
logger = logging.getLogger(__name__)


class FalconStaging(Staging, RepresentationMixin):

    def can_stage_in(self, file):
        """
        Returns True if the input file can be staged in, False otherwise.
        """
        logger.debug("Falcon checking file {}".format(repr(file)))
        return file.scheme == 'falcon'

    # def can_stage_out(self, file):
    #     """
    #     Returns True if the output file can be staged out, False otherwise.
    #     """
    #     logger.debug("Falcon checking file {}".format(repr(file)))
    #     return file.scheme == 'falcon'

    def stage_in(self, dm, executor, file, parent_fut):
        """
        Stages in a file using Falcon.

        Parameters:
        - dm: DataMover instance
        - executor: the executor to be used
        - file: the file to be staged in
        - parent_fut: the future representing the parent task

        Returns:
        - the future representing the staged in file
        """
        stage_in_app = self._falcon_stage_in_app(executor=executor, dfk=dm.dfk)
        app_fut = stage_in_app(outputs=[file], _parsl_staging_inhibit=True, parent_fut=parent_fut)
        return app_fut._outputs[0]

    # def stage_out(self, dm, executor, file, app_fu):
    #     """
    #     Stages out a file using Falcon.
    #
    #     Parameters:
    #     - dm: DataMover instance
    #     - executor: the executor to be used
    #     - file: the file to be staged out
    #     - app_fu: the future representing the application that produced the file
    #
    #     Returns:
    #     - the future representing the staged out file
    #     """
    #     stage_out_app = self._falcon_stage_out_app(executor=executor, dfk=dm.dfk)
    #     return stage_out_app(app_fu, _parsl_staging_inhibit=True, inputs=[file])

    def __init__(self, endpoint_ip: Optional[str] = "127.0.0.1"):
        """
        Initializes a FalconStaging instance.
        """
        self.endpoint_ip = endpoint_ip
        self.falcon = None

    def _falcon_stage_in_app(self, executor, dfk):
        """
        Returns a Parsl app that stages in a file using Falcon.

        Parameters:
        - executor: the executor to be used
        - dfk: the data flow kernel

        Returns:
        - a Parsl app that stages in a file using Falcon
        """
        executor_obj = dfk.executors[executor]
        f = partial(_falcon_stage_in, self, executor_obj)
        return python_app(executors=['_parsl_internal'], data_flow_kernel=dfk)(f)

    # def _falcon_stage_out_app(self, executor, dfk):
    #     """
    #     Returns a Parsl app that stages out a file using Falcon.
    #
    #     Parameters:
    #     - executor: the executor to be used
    #     - dfk: the data flow kernel
    #
    #     Returns:
    #     - a Parsl app that stages out a file using Falcon
    #     """
    #     executor_obj = dfk.executors[executor]
    #     f = partial(_falcon_stage_out, self, executor_obj)
    #     return python_app(executors=['_parsl_internal'], data_flow_kernel=dfk)(f)

    def initialize_transfer(self, working_dir):
        sender_command = ["falcon", "receiver", "--host", self.endpoint_ip, "--port", "5000", "--data_dir",
                          working_dir]
        print(sender_command)
        try:
            subprocess.run(sender_command, check=True)
        except subprocess.CalledProcessError as e:
            print(f"Command failed with exit code {e.returncode}: {e.stderr}")
        except FileNotFoundError:
            print("The 'falcon' command was not found. Make sure it's installed and in your system's PATH.")


def _falcon_stage_in(provider, executor, parent_fut=None, outputs=[], _parsl_staging_inhibit=True):
    # Initialize the transfer
    provider.initialize_transfer(executor.working_dir)


# def _falcon_stage_out(provider, executor, app_fu, inputs=[], _parsl_staging_inhibit=True):
#     # Initialize the transfer
#     provider.initialize_transfer(executor.working_dir)
