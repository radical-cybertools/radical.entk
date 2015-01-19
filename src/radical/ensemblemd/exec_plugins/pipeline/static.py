#!/usr/bin/env python

"""A static execution plugin for single tasks.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import saga
import pickle
import datetime
import radical.pilot

from radical.ensemblemd.exceptions import NotImplementedError, EnsemblemdError
from radical.ensemblemd.exec_plugins.plugin_base import PluginBase

# ------------------------------------------------------------------------------
#
_PLUGIN_INFO = {
    "name":         "pipeline.static.default",
    "pattern":      "Pipeline",
    "context_type": "Static"
}

_PLUGIN_OPTIONS = []


# ------------------------------------------------------------------------------
#
class Plugin(PluginBase):

    # --------------------------------------------------------------------------
    #
    def __init__(self):
        super(Plugin, self).__init__(_PLUGIN_INFO, _PLUGIN_OPTIONS)

    # --------------------------------------------------------------------------
    #
    def verify_pattern(self, pattern, resource):
        self.get_logger().info("Verifying pattern...")

    # --------------------------------------------------------------------------
    #
    def execute_pattern(self, pattern, resource):


        #-----------------------------------------------------------------------
        #
        def unit_state_cb (unit, state) :

            if state == radical.pilot.DONE:
                self.get_logger().info("Task with ID {0} has completed.".format(unit.uid))

            if state == radical.pilot.FAILED:
                self.get_logger().error("Task with ID {0} failed: STDERR: {1}, STDOUT: {2}".format(unit.uid, unit.stderr, unit.stdout))

        pipeline_instances = pattern.instances

        self.get_logger().info("Executing {0} pipeline instances on {1} allocated core(s) on '{2}'".format(
            pipeline_instances, resource._cores, resource._resource_key))

        try:
            resource._umgr.register_callback(unit_state_cb)

            # We use the journal to keep track of the stage / instance to
            # job mapping as well as to record associated timing informations.
            journal= {}
            steps = 0

            self.get_logger().info("Launched {0}-core pilot on {1}.".format(resource._cores, resource._resource_key))

            working_dirs = {}

            # Iterate over the different steps.
            for step in range(1, 64):
                # Create a new empty journal entry for the step.
                journal["step_{0}".format(step)] = {}

                # Get the method names
                s_meth = getattr(pattern, 'step_{0}'.format(step))

                # Build up the working dir bookkeeping structure.
                working_dirs["step_{0}".format(step)] = {}

                try:
                    kernel = s_meth(0)
                    steps += 1
                except NotImplementedError, ex:
                    # Not implemented means there are no further steps.
                    break

                for instance in range(1, pipeline_instances+1):

                    # Create a new journal entry for the instance.
                    journal["step_{0}".format(step)]["instance_{0}".format(instance)] = {}
                    inst_entry = journal["step_{0}".format(step)]["instance_{0}".format(instance)]

                    kernel = s_meth(instance)
                    kernel._bind_to_resource(resource._resource_key)

                    cud = radical.pilot.ComputeUnitDescription()

                    cud.pre_exec       = kernel._cu_def_pre_exec
                    cud.executable     = kernel._cu_def_executable
                    cud.arguments      = kernel.arguments
                    cud.mpi            = kernel.uses_mpi
                    cud.input_staging  = kernel._cu_def_input_data
                    cud.output_staging = kernel._cu_def_output_data

                    inst_entry["unit_description"] = cud
                    inst_entry["compute_unit"] = None
                    inst_entry["working_dir"] = "WORKDIR-s%s-i%s" % (step, instance)

                    self.get_logger().debug("Created step_1 CU ({0}/{1}): {2}.".format(instance+1, pipeline_instances, cud.as_dict()))

                self.get_logger().info("Created {0} ComputeUnits for pipeline step {1}.".format(pipeline_instances, step))

            # -----------------------------------------------------------------
            # At this point, we have created a complete journal for the
            # pattern instance. We can now start 'enacting' it through
            # radical pilot.
            for step in range(1, steps+1):
                step_key = "step_%s" % step

                for instance in range(1, pipeline_instances+1):
                    instance_key = "instance_%s" % instance

                    step_units = []

                    # Now we can replace the data-linkage placeholders.
                    jd = journal[step_key][instance_key]["unit_description"]
                    for pe in jd.pre_exec:

                        for i in range(1,33):
                            placeholder = "$STEP_{step}".format(step=i)
                            journal_key = "step_{step}".format(step=i)

                            if placeholder in pe:
                                jd.pre_exec.remove(pe)
                                expanded_pe = pe.replace(placeholder, journal[journal_key][instance_key]["working_dir"])
                                jd.pre_exec.append(expanded_pe)

                    unit = resource._umgr.submit_units(jd)
                    step_units.append(unit)
                    journal[step_key][instance_key]["compute_unit"] = unit

                self.get_logger().info("Submitted ComputeUnits for pipeline step {0}.".format(step))
                resource._umgr.wait_units()

                # Update all working directories so they can be accessed from
                # the next step(s).
                for instance in range(1, pipeline_instances+1):
                    instance_key = "instance_%s" % instance
                    work_dir = saga.Url(journal[step_key][instance_key]["compute_unit"].working_directory).path
                    journal[step_key][instance_key]["working_dir"] = work_dir

                failed_units = ""
                for unit in step_units:
                    if unit.state != radical.pilot.DONE:
                        failed_units += " * Compute Unit {0} failed with an error: {1}\n".format(unit.uid, unit.stderr)

                if len(failed_units) > 0:
                    raise EnsemblemdError("One or more ComputeUnits failed in pipeline step {0}: \n{1}".format(step, failed_units))

        except Exception, ex:
            self.get_logger().exception("Fatal error during execution: {0}.".format(str(ex)))
            raise

        finally:
            self.get_logger().info("Deallocating resource.")
            resource.deallocate()

        # -----------------------------------------------------------------
        # At this point, we have executed the pattern succesfully. Now,
        # if profiling is enabled, we can write the profiling data to
        # a file.
        do_profile = os.getenv('RADICAL_ENDM_PROFILING', '0')

        if do_profile != '0':

            outfile = "execution_profile_{time}.csv".format(time=datetime.datetime.now().isoformat())
            self.get_logger().info("Saving execution profile in {outfile}".format(outfile=outfile))

            with open(outfile, 'w+') as f:
                # General format of a profiling file is row based and follows the
                # structure <unit id>; <s_time>; <stop_t>; <tag1>; <tag2>; ...
                head = "task; start_time; stop_time; step; iteration"
                f.write("{row}\n".format(row=head))

                for step in journal.keys():
                    for iteration in journal[step].keys():
                        data = journal[step][iteration]
                        cu = data["compute_unit"]

                        row = "{uid}; {start_time}; {stop_time}; {tags}".format(
                            uid=cu.uid,
                            start_time=cu.start_time,
                            stop_time=cu.stop_time,
                            tags="{step}; {iteration}".format(step=step.split('_')[1], iteration=iteration.split('_')[1])
                        )
                        f.write("{row}\n".format(row=row))
