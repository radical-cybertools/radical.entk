Performance Profiling
=====================

During development of a new Ensemble MD application, you will probably
come to the point where you want to develop a deeper understanding of how your
application behaves performance wise.

Understanding an application's performance is the key to future design decisions
and performance optimizations. Ensemble MD provides a  simple set of tools
to profile an application.

Performance profiling doesn't require any modification of your application
code and is entirely controlled via environment variables. This way you can
turn profiling off and on on-demand throughout the development process.

To get started, simply set the ``RADICAL_ENDM_PROFILING`` variable to ``1``
and run your application::

    RADICAL_ENDM_PROFILING=1 python examples/pipeline.py

Once your application has finished running, you will find a CSV file
`execution_profile_{time}.csv` (where time is the time of creation)
in your working directory containing some key execution timing.

Profile Visualization
---------------------

You can use a tool of your choice to vizualize the data. For example, GnuPlot:

```
GNUPLOT EXAMPLE
```
