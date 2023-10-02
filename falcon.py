import logging
from functools import partial
import subprocess

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

    def initialize_transfer(self, file):
        sender_command = ["falcon", "receiver", "--host", file.netloc, "--port", "5000", "--data_dir",
                          file.path]
        print(sender_command)
        try:
            subprocess.run(sender_command, check=True)
        except subprocess.CalledProcessError as e:
            print(f"Command failed with exit code {e.returncode}: {e.stderr}")
        except FileNotFoundError:
            print("The 'falcon' command was not found. Make sure it's installed and in your system's PATH.")


def _falcon_stage_in(provider, executor, parent_fut=None, outputs=[], _parsl_staging_inhibit=True):
    # Initialize the transfer
    file = outputs[0]
    provider.initialize_transfer(file)
