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

Some mods:

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

## Executing the example (local execution)

```bash
cd $HOME
cd radical.ensemblemd/examples/usecase_seisflow/regional_Greece_small
RADICAL_ENTK_VERBOSE=info python runme.py
```

## Looking at the output

The output is produced in ```$HOME/radical.pilot.sandbox```. Check the last folder created in this 
directory starting with ```rp.session.*```. There exist folders named ```unit.00*``` representing each task. The folder contains the actual commands executed in a shell script, standard error, standard output and any input, intermediate and output data of the executed task.


## Executing the example on Stampede 

* You need to have gsissh-access to Stampede. Please see notes here: ```https://github.com/vivek-bala/docs/blob/master/gsissh_setup_stampede_ubuntu_xenial.sh```. This are notes to help in the procedure and not guaranteed to work as the specifics might be system dependent. Happy to help on this.

You can verify passwordless access by running ```gsissh -p 2222 <username>@stampede.tacc.xsede.org``` at the end which should **not** prompt you for a password. 

* Open ```runme.py``` under ```radical.ensemblemd/examples/usecase_seisflow/regional_Greece_small```. Add your stampede username and password in **line 62**.


```bash
cd $HOME
cd radical.ensemblemd/examples/usecase_seisflow/regional_Greece_small
RADICAL_ENTK_VERBOSE=info python runme.py --resource xsede.stampede
```

By default, you have acquired 16 cores in total (line 62) and you run 4 tasks (line 9) each using 4 cores (line 24, 34). So all tasks execute at the same time. To observe the dynamic capability of RADICAL Pilot to handle more tasks than can be executed at a given time, increase the ensemble size to 16 (line 9) or number of cores per task to 8 (line 24, 34; you might have to change some parameters input_data/DATA/Par_file). There is no extra effort required by the user in both these cases.