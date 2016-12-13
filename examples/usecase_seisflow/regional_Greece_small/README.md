# Simpy workflow with EnTK

## Requirements (local machine)

* python >= 2.7 (< 3.0)
* python-pip
* python-virtualenv
* git

## Installation

* The instructions in this document are relative to $HOME, feel free to pick your own data locations.

* You will first need to create a virtual environment. This is to avoid conflict between packages required for this example and local system packages. 

```bash
virtualenv $HOME/myenv
source $HOME/myenv/bin/activate
```

* You now need to install specific branches of [radical.pilot](https://github.com/radical-cybertools/radical.pilot) and [radical.ensemblemd](https://github.com/radical-cybertools/radical.ensemblemd). Some of the features required for this project are in the development branch and would be released soon.


Radical pilot installation:

```bash
cd $HOME
git clone https://github.com/radical-cybertools/radical.pilot.git
cd radical.pilot
git checkout usecase/vivek
pip install .
```

Some modes:

* Comment out **line 26** (specific RP paremeter):

```bash
vi $HOME/myenv/lib/python2.7/site-packages/radical/pilot/configs/agent_default.json
```

* Change "MPIRUN" to "MPIEXEC" in **line 24** of ```$HOME/myenv/lib/python2.7/site-packages/radical/pilot/configs/resource_local.json```


Ensemble toolkit installation:

```bash
cd $HOME
git clone https://github.com/radical-cybertools/radical.ensemblemd.git
cd radical.ensemblemd
git checkout usecase/seisflow
pip install .
```

Specfem installation:

Installation was done according to ```http://specfem3d-globe.readthedocs.io/en/latest/```. 

Binaries are present under ```radical.ensemblemd/examples/usecase_seisflow/regional_Greece_small/input_data/bin```. These might not work since they are compiled on another machine. Please compile  the four binaries (under the above path) according to the documentation in the above link and overwrite them over the ones in the bin/ folder. 

Once the above is done please run the ```prep_data.sh``` script.

## Executing the example (local execution only now)

```bash
cd $HOME
cd radical.ensemblemd/examples/usecase_seisflow/regional_Greece_small
RADICAL_ENTK_VERBOSE=info python runme.py
```

## Looking at the output

The output is produced in ```$HOME/radical.pilot.sandbox```. Check the last folder created in this 
directory starting with ```rp.session.*```. There exist folders named ```unit.00*``` representing each task. The folder contains the actual commands executed in a shell script, standard error, standard output and any input, intermediate and output data of the executed task.