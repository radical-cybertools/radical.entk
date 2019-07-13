
  - For a list of bug fixes, see
    https://github.com/radical-cybertools/radical.entk/ \
            issues?q=is%3Aissue+is%3Aclosed+sort%3Aupdated-desc
  - For a list of open issues and known problems, see
    https://github.com/radical-cybertools/radical.entk/ \
            issues?q=is%3Aissue+is%3Aopen+


0.70.0  Release                                                       2019-07-07
--------------------------------------------------------------------------------

  - fix executable value check
  - Fix outdated command and session name
  - pep8
  - allow str, unicode and None for executable
  - executable list -> scalar


0.62.0  Release                                                       2019-06-08
--------------------------------------------------------------------------------

  - add travis support (pytest, coverage, flake8, pylint)


--------------------------------------------------------------------------------
## Changelog for 0.7.17

* Issues resolved in 0.7.17 milestone:
  https://github.com/radical-cybertools/radical.entk/milestone/15
* support pipeline suspend and resume


## Changelog for 0.7.17

* Issues resolved in 0.7.17 milestone:
  https://github.com/radical-cybertools/radical.entk/milestone/15
* support pipeline suspend and resume


## Changelog for 0.7.14+0.7.16 (hotfix)

* Issues resolved in 0.7.14 milestone:
  https://github.com/radical-cybertools/radical.entk/milestone/14
* Bug fixes and updated documentation


## Changelog for 0.7.12+0.7.13 (hotfix)

* Issues resolved in 0.7.12 milestone:
  https://github.com/radical-cybertools/radical.entk/milestone/13
* Fixed bug in task manager where there were some elements blocking the pika communication loop
* Improved documentation


## Changelog for 0.7.11

* Issues resolved in 0.7.11 milestone:
  https://github.com/radical-cybertools/radical.entk/milestone/12
* Separated the CU creation from RMQ Communication thread

## Changelog for 0.7.9

* Issues resolved in 0.7.9 milestone:
  https://github.com/radical-cybertools/radical.entk/milestone/11?closed=1
* Included ability to suspend and resume pipeline execution
* Several bug fixes

## Changelog for 0.7.8

* Hotfix for issue 259

## Changelog for 0.7.7

* Issues as part of 0.7.7 milestone:
  https://github.com/radical-cybertools/radical.entk/milestone/10/
* Improved test coverage

## Changelog for 0.7.6

* Minor fixes to be able to upload to conda repo

## Changelog for 0.7.5

* Issues as part of 0.7.5 milestone:
  https://github.com/radical-cybertools/radical.entk/milestone/9/
* Documentation improved with more examples
* Bug fixes

## Changelog for 0.7.4

* Bug fixes
* Improved documentation
* Improved test coverage

## Changelog for 0.7.3

* Bug fixes
* Improved documentation
* Improved test coverage

## Changelog for 0.7.0

* Issues as part of 0.7.0 milestone:
  https://github.com/radical-cybertools/radical.entk/milestone/8/
* API Changes:
  * 'cores' attribute of task changed to `cpu_reqs` and `gpu_reqs` which are
    dictionaries with the following structure:
    ```python
    task.cpu_reqs =     {
                            'processes': 1,
                            'process_type': None/MPI,
                            'threads_per_process': 1,
                            'thread_type': None/OpenMP
                        }

    task.gpu_reqs =     {
                            'processes': 1,
                            'process_type': None/MPI,
                            'threads_per_process': 1,
                            'thread_type': None/OpenMP
                        }
    ```
    * ResourceManager object is not exposed to the user. The resource
    description is to be provided to the AppManager using the 'resource_desc'
    attribute
    ```python
    amgr = AppManager()
    amgr.resource_desc = {
        'resource': 'local.localhost',
        'walltime': 10,
        'cpus': 1}
    ```
    * AppManager does not have assign_workflow() method. Instead you assign the
    workflow using the assignment operator (similar to resource desc) to the
    workflow attribute.
    ```python
    amgr = AppManager()
    amgr.workflow = pipelines
    ```
      Note: `pipelines` can be a list or a set of Pipeline objects but there are
      no guarantees of order
    * AppManager has two important additional arguments:
      * write_workflow (True/False) to write the executed workflow to a
        file post-termination
      * rmq_cleanup (True/False) to cleanup the rabbitmq queues post-execution
    * AppManager reads default values from a JSON config file. Expected config
      file structure:
    ```json
    {
    "hostname": "localhost",
    "port": 5672,
    "reattempts": 3,
    "resubmit_failed": false,
    "autoterminate": true,
    "write_workflow": false,
    "rts": "radical.pilot",
    "pending_qs": 1,
    "completed_qs": 1,
    "rmq_cleanup": true
    }
    ```
    * The ``shared_data`` attribute is part of the AppManager object now
      (since the resource manager is not exposed to the user anymore).
    ```python
    amgr = AppManager()
    amgr.shared_data = ['file1.txt','/tmp/file2.txt']
    ```

## Changelog for 0.6.3

* Added some more tests

## Changelog for 0.6.2

* Several [issues](https://github.com/radical-cybertools/radical.entk/issues?q=is%3Aopen+is%3Aissue+milestone%3A%22Release+0.6.2%22) addressed
* Bulk submission of tasks across entire set of pipelines
* Write workflow structure post-execution upon 'write_workflow=True'
  argument to AppManager

## Changelog for 0.6.1

* Multiple bug fixes

## Changelog for 0.6.0

* ```_parent_pipeline``` and ```_parent_stage``` on Stage and Task objects
  changed to ```parent_pipeline``` and ```parent_stage```.

