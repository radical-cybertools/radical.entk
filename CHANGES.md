
  - For a list of bug fixes, see
    https://github.com/radical-cybertools/radical.entk/ \
            issues?q=is%3Aissue+is%3Aclosed+sort%3Aupdated-desc
  - For a list of open issues and known problems, see
    https://github.com/radical-cybertools/radical.entk/ \
            issues?q=is%3Aissue+is%3Aopen+

1.42.0 Release                                                        2024-01-10
--------------------------------------------------------------------------------

  - maintenance


1.42.0 Release                                                        2023-12-04
--------------------------------------------------------------------------------

  - RTD fix
  - Consistent doc theme across RCT
  - switch to a thread based instead of process based tmgr


1.41.0 Release                                                        2023-10-17
--------------------------------------------------------------------------------

  - fix RTD
  - fix ZMQ bridge in `AppManager`
  - fix session handling for `TaskManager`


1.37.0 Release                                                        2023-09-23
--------------------------------------------------------------------------------

  - add means to annotate dataflow
  - extend coverage for Pipeline validation
  - update file id (`[task_uid:]file_name`) parsing procedure


1.36.0 Release                                                        2023-08-01
--------------------------------------------------------------------------------

  - added tool to extract the provenance graph (from Pipeline & from JSON)


1.34.0 Release                                                        2023-04-25
--------------------------------------------------------------------------------

  - added log messages to track TaskQueue
  - introduced service task
  - added mongodb reconnect for a thread with `rp.TaskManager`
  - added tests (service tasks, pilot cancelation)
  - extend num digits for `Task.uid`
  - fixed reattempts and sizing for `task_queue`
  - moved MDB reconnect into TMGR mp.Process (thread safety)
  - removed excessive log message (when queue task is empty)
  
  
1.33.0 Release                                                        2023-04-25
--------------------------------------------------------------------------------
  
  - enable GPU sharing (convert `gpu_process` from type `int` into `float`)
  - updated requirement for RP (>=1.22)


1.30.0 Release                                                        2023-02-01
--------------------------------------------------------------------------------
  
  - replace RMQ communication with ZMQ
  - add attribute `exclusive` to `Task.tags` (in sync with RP tags representation)
  - add scaling examples
  - comment some debug logs, always set exception attribs
  - fix Mock backend
  - inform RE about RP level task errors


1.20.0 Release                                                        2022-12-16
--------------------------------------------------------------------------------

  - bump python test env to 3.7
  - doc updates
  - synced with updated attributes in RP TaskDescription


1.18.0 Release                                                        2022-10-11
--------------------------------------------------------------------------------

  - added task attribute `environment`


1.17.0 Release                                                        2022-09-20
--------------------------------------------------------------------------------

  - added timeout for online version check


1.16.0 Release                                                        2022-08-15
--------------------------------------------------------------------------------

  - add tutorial examples
  - cleanup gitignore
  - deleted test related to ticket #255 - are already tested
  - deleted test related to ticket #270 - `heartbeat` test updated accordingly
  - introduced Task attributes `pre/post_launch`
  - move tutorial code to notebooks
  - removed leftovers in `test_issues` - all covered with other tests
  - set pilot cancellation through session close
  - updated `heartbeat_response` for TMGR


1.14.0  Release                                                       2022-04-13
--------------------------------------------------------------------------------

  - fix lingering uid error
  - update AppManager instance check in `prof_utils`
  - updated setup requirements


1.13.0  Release                                                       2022-03-21
--------------------------------------------------------------------------------

  - clean temporary setup files


1.12.0  Release                                                       2022-02-28
--------------------------------------------------------------------------------

  - make `Task` instances hashable
  - enforced that `uid` is set during initialization only
  - support `descr.memory`
  - fixed and updated class Task (Munch-based)
  - use ru.TypedDict for Munch, fix tests
  

1.11.0  Release                                                       2022-01-19
--------------------------------------------------------------------------------

  - expose mem_per_process in task object


1.9.0  Release                                                        2021-11-22
--------------------------------------------------------------------------------

  - support mem_per_process


1.8.0  Release                                                        2021-09-23
--------------------------------------------------------------------------------

  - 'config' param introduced in AppManager to take 'base_path' etc.
  - eliminating warning message for being offline
  - type conversion to str for PosixPath


1.6.7  Release                                                        2021-07-15
--------------------------------------------------------------------------------

  - support for RabbitMQ virtual host
  - update the license
  - x.name is replaced by x.uid to locate files


1.6.5  Release                                                        2021-04-15
--------------------------------------------------------------------------------

  - #572 Trying to revert to codecov action after resetting the token
  - #571 RP task processor adds default values to RP task cpu and gpu reqs
  - #568 Using RPs tags instead of tag
  - #567 Verifying a task can fit a resource
  - #566 Fix/integration tests
  - #569 Fixing conda installation instructions
  - #538 EnTK components failure resilience
  - #563 Adding stage_on_error. Issue #562
  - #557 Fix issue 477
  - #560 Feature/pilot failure

1.5.8  Release                                                        2020-12-15
--------------------------------------------------------------------------------

  - PR #525, Moving warning filter to the whole file
  - PR #522, Fix/integration tests
  - PR #519, Small refactoring changes
  - PR #518, AppManager raises error when there are multiple reattempts
  - PR #511, Documentation update
  - PR #510, move to relative imports, simplify code hierarchy
  - PR #508, Unit tests for methods that use threads and processes
  - PR #507, Feature/id mapping

1.5.7  Release                                                        2020-11-17
--------------------------------------------------------------------------------

  - PR #503, Deleting unnecessary files
  - PR #502, Feature/job name
  - PR #499, Additional unit tests
  - PR #498, Linting and fixing task path in appmanager
  - PR #497, Fix warning regarding ABC import from collections
  - PR #496, Fix/unittests
  - PR #494, Silence Integration and Issues tests
  - PR #492, Aligning thread/processes definitions between EnTK and RP
  - PR #491, Doc string for a method


1.5.5  Release                                                        2020-10-13
--------------------------------------------------------------------------------

  - PR #483, reuse rmq connections


1.5.1  Release                                                        2020-09-01
--------------------------------------------------------------------------------

  - Pylint, flake updated, PR #475


1.5.0  Release                                                        2020-08-24
--------------------------------------------------------------------------------

  - CI tests updated, PR #471
  - Task sync enhancement, PR #466


1.4.1  Release                                                        2020-07-17
--------------------------------------------------------------------------------

  - Documentation updated, PR #453, #451, #450, #446
  - Shared data fix #449


1.4.0  Release                                                        2020-05-18
--------------------------------------------------------------------------------

  - Early assignment of UIDs #435
  - Improved profile handling consistency #434


1.0.1  Release                                                        2020-02-13
--------------------------------------------------------------------------------

  - Documentation Update
    - Instructions with Python 3
    - Fix of API reference pages
  - Travis CI updated, removing unnecessary tests, updating flake8/pylint
  - Workaround for EnTK deadlock on non-trivial pipeline counts. #410


1.0.0  Release                                                        2019-12-26
--------------------------------------------------------------------------------

  - transition to Python3
  - add `appman.outputs`  (symmetric to `appman.shared_data`)
  - RMQ Auth (username/password) #379
  - Global TOC on sphinx navigation sidebar #390
  - fix for resume/suspend #376


0.72.1  Hotfix Release                                                2019-09-22
--------------------------------------------------------------------------------

  - forgot to merge :-P


0.72.0  Release                                                       2019-09-11
--------------------------------------------------------------------------------

  - fix test setup
  - Fix documentation
  - adaptivity examples updated, post_exec is callable now
  - add dict initialization for tasks
  - write all workflows in session description
  - battle test resource issues
  - fix channel cleanup/restart
  - fix workflow writer (again)


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

# ------------------------------------------------------------------------------

