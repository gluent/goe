# goe
Gluent Offload Engine

# Build
Simple steps to get a working Python
```
sudo apt-get install rustc
sudo apt-get install unixodbc-dev
python3 -m venv ./.venv
pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

# Development
Getting setup:
```
cd goe
source ./.venv/bin/activate
PYTHONPATH=${PWD}:${PWD}/scripts
```

Running an Offload:
```
cd scripts
./offload -t my.table
```

Running unit tests:
```
cd goe
pytest tests/unit
```
