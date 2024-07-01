************************
RADICAL-Ensemble Toolkit
************************

.. image:: https://img.shields.io/pypi/v/radical.entk.svg
   :target: https://pypi.python.org/pypi/radical.entk
   :alt: PyPI Package
.. image:: https://anaconda.org/conda-forge/radical.entk/badges/version.svg
   :target: https://anaconda.org/conda-forge/radical.entk
   :alt: Conda Version
.. image:: https://img.shields.io/pypi/l/radical.entk.svg
   :target: https://pypi.python.org/pypi/radical.entk
   :alt: License
.. image:: https://readthedocs.org/projects/radicalentk/badge/?version=stable
   :target: https://radicalentk.readthedocs.io/en/stable/?badge=stable
   :alt: Documentation Status
.. image:: https://github.com/radical-cybertools/radical.entk/actions/workflows/python-app.yml/badge.svg
   :target: https://github.com/radical-cybertools/radical.entk/actions/workflows/python-app.yml
   :alt: Build Status
.. image:: https://codecov.io/gh/radical-cybertools/radical.entk/branch/devel/graph/badge.svg?token=dHn74ChzmX
   :target: https://codecov.io/gh/radical-cybertools/radical.entk
   :alt: Test Coverage

RADICAL Ensemble Toolkit (EnTK) is a Python library for developing and executing
large-scale ensemble-based workflows. Different from a general-purpose workflow
system, EnTK exposes a programming application interface (API) specifically
designed to concisely describe applications in which compute tasks are grouped
into concurrent or sequential pipelines. Each pipeline is a sequence of stages
and each stage is a set of tasks. Tasks can be arbitrary executables or Python
functions, requiring single/multi core/GPU/node or MPI/OpenMP. EnTK uses
`RADICAL-Pilot <https://radicalpilot.readthedocs.io/en/devel/>`_, enabling the
execution of workflows are scale on diverse HPC platforms. Ensemble-based
workflows are common in many scientific domains, including drug discovery,
molecular modeling, material engineering and climate science.

* Repository: https://github.com/radical-cybertools/radical.entk
* Issues: https://github.com/radical-cybertools/radical.entk/issues

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   introduction
   entk
   install
   user_guide
   examples
   advanced_examples
   api
   exceptions
   dev_docs


   getting_started.ipynb
   tutorials.rst
   supported.rst
   envs.rst
   glossary.rst
   internals.rst
   apidoc.rst
   release_notes.md


.. note:: Please use the following to reference Ensemble Toolkit:

   Balasubramanian, Vivek, Matteo Turilli, Weiming Hu, Matthieu Lefebvre, Wenjie
   Lei, Ryan Modrak, Guido Cervone, Jeroen Tromp, and Shantenu Jha. "Harnessing
   the power of many: Extensible toolkit for scalable ensemble applications." In
   *2018 IEEE international parallel and distributed processing symposium
   (IPDPS)*, pp. 536-545. IEEE, 2018.