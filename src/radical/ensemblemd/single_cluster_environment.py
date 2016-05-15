#!/usr/bin/env python

"""TODO: Docstring.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
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

		self._logger  = ru.get_logger('radical.entk.SingleClusterEnvironment')
		self._reporter = ru.LogReporter(name='radical.entk.SingleClusterEnvironment')

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
		profiling = int(os.environ.get('RADICAL_ENMD_PROFILING',0))
		self._reporter.info('\nStarting Deallocation..\n')
		if profiling == 1:
			start_time = datetime.datetime.now()

		self.get_logger().info("Deallocating Cluster")

		if self._exctype != None:
			self.get_logger().error("Fatal error during execution: {0}.".format(str(self._excvalue)))
			self._reporter.error("Fatal error: {0}.".format(str(self._excvalue)))
			traceback.print_tb(self._traceback)
		

		self._session.close(cleanup=self._cleanup)
		self._reporter.ok('>>done \n')    

		profiling = int(os.environ.get('RADICAL_ENMD_PROFILING',0))
		if profiling == 1:
			stop_time = datetime.datetime.now()
			f1 = open('enmd_core_overhead.csv','a')
			f1.write('deallocate,start_time,{0}\n'.format(start_time))
			f1.write('deallocate,stop_time,{0}\n'.format(stop_time))
			f1.close()

			f1 = open('pilot_profile_{mysession}.csv'.format(mysession=self._session.uid),'w')
			title = "uid, New, PendingLaunch, Launching, PendingActive, Active, Canceled, Done"
			f1.write(title + "\n\n")
			st_data = {}
			for st in self._pilot.state_history:
				st_dict = st.as_dict()
				st_data["{0}".format( st_dict["state"] )] = {}
				st_data["{0}".format( st_dict["state"] )] = st_dict["timestamp"]

			states = ['New','PendingLaunch','Launching','PendingActive','Active','Canceled','Done']

			for state in states:
				if (state in st_data) is False:
					st_data[state] = None

			line = "{uid}, {New}, {PendingLaunch}, {Launching}, {PendingActive}, {Active}, {Canceled}, {Done}".format(
							uid=self._pilot.uid,
							New=st_data['New'],
							PendingLaunch=st_data['PendingLaunch'],
							Launching=(st_data['Launching']),
							PendingActive=(st_data['PendingActive']),
							Active=(st_data['Active']),
							Canceled=(st_data['Canceled']),
							Done=(st_data['Done']),
						)
			f1.write(line + '\n')

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
		if profiling == 1:
			start_time = datetime.datetime.now()

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

			

			if profiling == 1:
				stop_time = datetime.datetime.now()

			self._reporter.ok('>> ok')

		except Exception, ex:
			self.get_logger().exception("Fatal error during resource allocation: {0}.".format(str(ex)))
			self._reporter.error('Allocation failed: {0}'.format(str(ex)))
			if self._session:
				self._session.close()
			raise

		finally:
			if profiling == 1:
				title = 'step,probe,timestamp'
				f1 = open('enmd_core_overhead.csv','w')
				f1.write(title+'\n\n')
				f1.write('allocate,start_time,{0}\n'.format(start_time))
				f1.write('allocate,stop_time,{0}\n'.format(stop_time))
				f1.close()

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
			plugin.execute_pattern(pattern, self)
		except KeyboardInterrupt:
			self._exctype,self._excvalue,self._traceback = sys.exc_info()           
			self.get_logger().error("Fatal error during execution: {0}.".format(str(self._excvalue))) 
			self._reporter.error("Fatal error during execution: {0}.".format(str(self._excvalue)))
		except Exception, ex:
			self._exctype,self._excvalue,self._traceback = sys.exc_info()
			self.get_logger().error("Fatal error during execution: {0}.".format(str(self._excvalue)))
			self._reporter.error("Fatal error during execution: {0}.".format(str(self._excvalue)))
