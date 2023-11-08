# Datamover2Parsl

## Usage

1. Please create virtual environments on both source and destination server. For exmaple: run `python3 -m venv <venv_dir>`
2. Activate virtual environment: run `source <venv_dir>/bin/activate`
3. Install required python packages: `pip3 install -r requirements.txt`
4. On the destination server, please edit and run `python3 Test/Falcon.py`
5. On the source server(s), please run `python3 Sender/sender.py`
