Running Custom Protcol Processor
================================

Processing payments from the "MPS7" mainframe output is trivial with the include command line parser.


Installation
------------

The CLI parser assumes that you are running Python 3.7+. You can view the current version of python like so:

```
python --version
```


If you don't have Python 3.7 installed,
[pyenv](https://github.com/pyenv/pyenv) is recommended for python interpreter version management. 

Once you have pyenv installed, you can install Python 3.7 like so:

```
pyenv install 3.7.6
```

You can then either run `pyenv --global 3.7.6` to change the global version or you can run `pyenv --local 3.7.6` 
from within the project directory to set the version specifically for that directory and subdirectories. 

Re-run `python --version` to confirm that you have a valid python interpreter configured.

The CLI tool can be downloaded locally like so:

```
git clone https://github.com/scuerda/ad-hoc-proto.git
cd ad-hoc-proto
```


No dependencies are required, so you shouldn't need to setup/activate a virtual environment.

Basic Use
---------

You can get the answers asked for in the task by passing in an input file and a user id like so:

```
python parse.py --input txnlog.dat --user_id 2456938384156277127
```

You can output a csv with the decoded transactions by passing an output file path like so:


```
python parse.py --input txnlog.dat --output report.csv --user_id 2456938384156277127
```


To view the available cli flags:

```
python parse.py --help
```

Running Tests
-------------

Tests can be run like so:

```
python -m unittest
```
