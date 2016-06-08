#!/usr/bin/env python

"""TODO: Docstring.
"""

__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import traceback
import datetime
import radical.pilot
import radical.utils as ru

from radical.ensemblemd import version
from radical.ensemblemd.engine import Engine
from radical.ensemblemd.exceptions import EnsemblemdError, TypeError
from radical.ensemblemd.execution_pattern import ExecutionPattern
from radical.ensemblemd.execution_context import ExecutionContext

CONTEXT_NAME = "Static"

#-------------------------------------------------------------------------------
#
class SingleClusterEnvironment(ExecutionContext):
	"""A static execution context provides a fixed set of computational
	   resources.
	"""

	#---------------------------------------------------------------------------
	#
	def __init__(self, 
				 resource, 
				 cores, 
				 walltime, 
				 queue=None,
				 username=None, 
				 project=None, 
				 cleanup=False, 
				 database_url=None, 
				 database_name=None,
				 access_schema=None):
		"""Creates a new ExecutionContext instance.
		"""
		self._allocate_called = False
		self._umgr = None
		self._session = None
		self._pilot = None
		self._pmgr = None
		self._exctype = None
		self._excvalue = None
		self._traceback = None

		self._resource_key = resource
		self._queue = queue
		self._cores = cores
		self._walltime = walltime
		self._username = username
		self._project = project
		self._cleanup = cleanup
		self._database_url = database_url
		self._database_name = database_name
		self._schema = access_schema


		#shared data
		self._shared_data = None

		self._logger  = ru.get_logger('radical.enmd.SingleClusterEnvironment')
		self._reporter = ru.LogReporter(name='radical.enmd.SingleClusterEnvironment')

		# Profiling
		self._profiling = int(os.environ.get('RADICAL_ENMD_PROFILING',0))		
		self._profile_entities = {}
		self._num_patterns = 0
		self._core_ov_dict = {}

		if self._profiling == 1:
			os.environ["RADICAL_PILOT_PROFILE"] = "True"

		super(SingleClusterEnvironment, self).__init__()

	# --------------------------------------------------------------------------
	#
	def get_logger(self):
		return self._logger

	#---------------------------------------------------------------------------
	#
	@property
	def name(self):
		"""Returns the name of the execution context.
		"""
		return CONTEXT_NAME


	@property
	def shared_data(self):
		return self._shared_data

	@shared_data.setter
	def shared_data(self,data):
		self._shared_data = data
	

	#---------------------------------------------------------------------------
	#
	def deallocate(self):
		"""Deallocates the resources.
		"""
		
		self._reporter.info('\nStarting Deallocation..\n')

		if self._profiling == 1:
			self._core_ov_dict['dealloc_start'] = datetime.datetime.now()

		self.get_logger().info("Deallocating Cluster")

		if self._exctype != None:
			self.get_logger().error("Fatal error during execution: {0}.".format(str(self._excvalue)))
			self._reporter.error("Fatal error: {0}.".format(str(self._excvalue)))
			traceback.print_tb(self._traceback)
		

		self._session.close(cleanup=self._cleanup)
		self._reporter.ok('>>done \n')    

		if self._profiling == 1:
			self._core_ov_dict['dealloc_stop'] = datetime.datetime.now()
		
	#---------------------------------------------------------------------------
	#
	def allocate(self, wait=False):
		"""Allocates the requested resources.
		"""
		#-----------------------------------------------------------------------
		#
		def pilot_state_cb (pilot, state) :
			self.get_logger().info("Resource {0} state has changed to {1}".format(self._resource_key, state))

			if state == radical.pilot.FAILED:
				self.get_logger().error("Resource error: ")
				self.get_logger().error("Pattern execution FAILED.")
				sys.exit(1)

			if state == radical.pilot.DONE:
				self.get_logger().info("Resource allocation time over.")
				self._reporter.info('Resource allocation time over.')

			if state == radical.pilot.CANCELED:
				self.get_logger().info("Resource allocation cancelled.")
				self._reporter.info('Resource allocation cancelled.')



		self._allocate_called = True

		# Here we start the pilot(s).
		self._reporter.title('EnsembleMD (%s)' % version)

		self._reporter.info('Starting Allocation')

		profiling = int(os.environ.get('RADICAL_ENMD_PROFILING',0))

		if self._profiling == 1:
			self._core_ov_dict["alloc_start"] = datetime.datetime.now()

		if not self._database_url:
			self._database_url = os.getenv ("RADICAL_PILOT_DBURL", None)

		if  not self._database_url :
			raise PilotException ("no database URL (set RADICAL_PILOT_DBURL)")  

		if self._database_name is None:
			self._session = radical.pilot.Session(database_url=self._database_url)
		else:
			db_url = self._database_url + '/' + self._database_name
			self._session = radical.pilot.Session(database_url=db_url)

		try:

			if self._username is not None:
				# Add an ssh identity to the session.
				c = radical.pilot.Context('ssh')
				c.user_id = self._username
				self._session.add_context(c)

			pmgr = radical.pilot.PilotManager(session=self._session)
			pmgr.register_callback(pilot_state_cb)
			self._pmgr = pmgr

			pdesc = radical.pilot.ComputePilotDescription()
			pdesc.resource = self._resource_key
			pdesc.runtime  = self._walltime
			pdesc.cores    = self._cores

			if self._queue is not None:
				pdesc.queue = self._queue

			pdesc.cleanup = self._cleanup

			if self._project is not None:
				pdesc.project = self._project

			pdesc.access_schema = self._schema

			self.get_logger().info("Requesting resources on {0}".format(self._resource_key))

			self._pilot = pmgr.submit_pilots(pdesc)
			self.get_logger().info("Launched {0}-core pilot on {1}.".format(self._cores, self._resource_key))

			if self._shared_data is not None:
				self.get_logger().info("Commencing transfer of shared data to {0}".format(self._resource_key))
				shared_list = []
				for f in self._shared_data:
					if f.startswith('.'):
						f = os.getcwd() + f.split('.')[1] + '.' + f.split('.')[2]
					shared_dict =   {
										'source': 'file://%s'%f,
										'target': 'staging:///%s' %os.path.basename(f),
										'action': radical.pilot.TRANSFER
									}

					shared_list.append(shared_dict)

				self._pilot.stage_in(shared_list)

			if wait is True:
				self._pilot.wait(radical.pilot.ACTIVE)

			self._umgr = radical.pilot.UnitManager(
				session=self._session,
				scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION)

			self._umgr.add_pilots(self._pilot)

			if self._profiling == 1:
				alloc_stop_time = datetime.datetime.now()

			self._reporter.ok('>> ok')

		except Exception, ex:
			self.get_logger().exception("Fatal error during resource allocation: {0}.".format(str(ex)))
			self._reporter.error('Allocation failed: {0}'.format(str(ex)))
			if self._session:
				self._session.close()
			raise

		finally:
			if self._profiling == 1:
				self._core_ov_dict["alloc_stop"] = datetime.datetime.now()

	#---------------------------------------------------------------------------
	#
	def run(self, pattern, force_plugin=None):
		"""Creates a new SingleClusterEnvironment instance.
		"""
		# Make sure resources were allocated.
		if self._allocate_called is False:
			raise EnsemblemdError(
				msg="Resource(s) not allocated. Call allocate() first."
			)

		# Some basic type checks.
		if not isinstance(pattern, ExecutionPattern):
			raise TypeError(
			  expected_type=ExecutionPattern,
			  actual_type=type(pattern))

		self._engine = Engine()
		plugin = self._engine.get_execution_plugin_for_pattern(
			pattern_name=pattern.name,
			context_name=self.name,
			plugin_name=force_plugin)


		self._reporter.info('\nVerifying pattern')
		plugin.verify_pattern(pattern, self)
		self._reporter.ok('>>ok')
		try:
			self._reporter.info('\nStarting pattern execution')
			self._num_patterns += 1
			execution_profile = plugin.execute_pattern(pattern, self)
			if execution_profile is not None:
				pat_overhead_dict, cu_dict = execution_profile
				self._profile_entities[pattern] = [pat_overhead_dict, cu_dict, self._num_patterns]
		except KeyboardInterrupt:
			self._exctype,self._excvalue,self._traceback = sys.exc_info()           
			self.get_logger().error("Fatal error during execution: {0}.".format(str(self._excvalue))) 
			self._reporter.error("Fatal error during execution: {0}.".format(str(self._excvalue)))
		except Exception, ex:
			self._exctype,self._excvalue,self._traceback = sys.exc_info()
			self.get_logger().error("Fatal error during execution: {0}.".format(str(self._excvalue)))
			self._reporter.error("Fatal error during execution: {0}.".format(str(self._excvalue)))

	def profile(self, pattern):

		if not self._profiling:
			print 'Profiling variable not set. Please set RADICAL_ENMD_PROFILING=1 and rerun.'
			sys.exit(1)
		## ------------------------------------------------------------------------------------------------------------------------
		## Profile -- Pilot

		import radical.pilot.utils as rpu

		sid        		= self._session.uid
		profiles 	= rpu.fetch_profiles(sid=sid, tgt='/tmp/')
		profile    	= rpu.combine_profiles (profiles)
		frame      	= rpu.prof2frame(profile)
		sf, pf, uf 	= rpu.split_frame(frame)

		rpu.add_info(uf)
		rpu.add_states(uf)
		s_frame, p_frame, u_frame = rpu.get_session_frames(sid)

		# Save Pilot DF to csv file
		p_frame.to_csv("pilot_profile_{mysession}.csv".format(mysession=self._session.uid))

		## ------------------------------------------------------------------------------------------------------------------------

		## ------------------------------------------------------------------------------------------------------------------------
		## Profile -- EnMD Core Overhead

		title = 'step,probe,timestamp'
		f1 = open('enmd_core_overhead.csv','w')
		f1.write(title+'\n\n')

		for key, val in self._core_ov_dict.iteritems():

			f1.write("{0},{1},{2}\n".format(
				key.split("_")[0],
				key.split("_")[1],
				val))

		f1.close()

		## ------------------------------------------------------------------------------------------------------------------------

		## Profile -- SAL pattern
		if pattern.name == "SimulationAnalysisLoop":

			## ------------------------------------------------------------------------------------------------------------------------
			## Profile  -- SAL

			from profiler.simulation_analysis_loop import pattern_profiler
			from profiler.simulation_analysis_loop import exec_profiler

			pattern_overhead_dict, cu_dict, pat_num = self._profile_entities[pattern]
			pat_iters = pattern.iterations
			pat_name = pattern.name

			## Profile -- EnTK pattern profile
			pattern_profiler(pattern_overhead_dict, pat_name, pat_num, pat_iters)			

			## Profile -- EnTK exec profiler
			exec_profiler(self._session.uid, cu_dict, pat_iters)
			## ------------------------------------------------------------------------------------------------------------------------

		
		elif pattern.name == "Pipeline":
			pass
			## ------------------------------------------------------------------------------------------------------------------------
			## Profile -- Pipeline

			from profiler.pipeline import pattern_profiler
			from profiler.pipeline import exec_profiler

			pattern_overhead_dict, cu_dict, pat_num = self._profile_entities[pattern]

			## ------------------------------------------------------------------------------------------------------------------------

			## ------------------------------------------------------------------------------------------------------------------------
			## Profile -- EnTK Pattern Overhead

			## ------------------------------------------------------------------------------------------------------------------------

			## ------------------------------------------------------------------------------------------------------------------------
			## Profile -- CU Execution profile -- Pipeline -- RP profiler

			## ------------------------------------------------------------------------------------------------------------------------

		elif pattern.name == "BagofTasks":

			## ------------------------------------------------------------------------------------------------------------------------
			## Profile -- BagofTasks

			from profiler.bag_of_tasks import pattern_profiler
			from profiler.bag_of_tasks import exec_profiler

			pattern_overhead_dict, cu_dict, pat_num = self._profile_entities[pattern]
			pat_name = pattern.name
			pat_steps = pattern.steps
			
			## Profile -- EnTK Pattern Overhead 
			pattern_profiler(pattern_overhead_dict, pat_name, pat_num)

			## Profile -- EnTK exec profiler
			exec_profiler(self._session.uid, cu_dict, pat_steps)
			## ------------------------------------------------------------------------------------------------------------------------


		elif pattern.name == "ReplicaExchange":
			pass
			## ------------------------------------------------------------------------------------------------------------------------
			## Profile -- EnMD Pattern Overhead -- Replica Exchange

			## ------------------------------------------------------------------------------------------------------------------------

			## ------------------------------------------------------------------------------------------------------------------------
			## Profile -- CU Execution profile -- Replica Exchange -- RP profiler

			## ------------------------------------------------------------------------------------------------------------------------