#!/usr/bin/env python

__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import traceback
import datetime
import radical.pilot
import radical.utils as ru

from radical.entk import version
from radical.entk.exceptions import EnTKError, TypeError
from radical.entk.execution_plugin.poe import PluginPoE

CONTEXT_NAME = "Static"

#-------------------------------------------------------------------------------
#
class ResourceHandle(object):
	'''Currently ResourceHandle provides access to only ONE computational resource - can be extended to multiple'''
	

	#---------------------------------------------------------------------------
	#
	def __init__(	self, 
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

		"""Creates a new ResourceHandle instance"""

		# User provided parameter assignments
		self._resource_key 	= resource
		self._cores 		= cores
		self._walltime 		= walltime
		self._queue 		= queue		
		self._username 	= username
		self._project 		= project
		self._cleanup 		= cleanup
		self._database_url 	= database_url
		self._database_name 	= database_name
		self._access_schema 	= access_schema

		# Internal parameters
		self._allocate_called 	= False
		self._umgr 		= None
		self._session 		= None
		self._pilot 		= None
		self._pmgr 		= None
		self._exctype 		= None
		self._excvalue 		= None
		self._traceback 		= None

		# Shared data
		self._shared_data = None

		# Logging parameters
		self._logger  		= ru.get_logger('radical.entk.ResourceHandle')
		self._reporter 		= self._logger.report

	# --------------------------------------------------------------------------
	
	def get_logger(self):
		return self._logger
	#---------------------------------------------------------------------------

	@property
	def name(self):
		"""Returns the name of the resource handle"""
		return CONTEXT_NAME
	#---------------------------------------------------------------------------

	@property
	def shared_data(self):
		return self._shared_data

	@shared_data.setter
	def shared_data(self,data):
		self._shared_data = data
	#---------------------------------------------------------------------------

	def deallocate(self):
		"""Deallocates the resources.
		"""
		
		self.get_logger().info("Deallocating Cluster")

		if self._exctype != None:
			self.get_logger().error("Fatal error during execution: {0}.".format(str(self._excvalue)))
			traceback.print_tb(self._traceback)
		

		self._session.close(cleanup=self._cleanup)
		#self._reporter.ok('>>done \n')    

	#---------------------------------------------------------------------------
	
	def allocate(self, wait=False):
		"""Allocates the requested resources -- cannot run without allocating resources"""

		def pilot_state_cb (pilot, state) :
			self.get_logger().info("Pilot {2} on resource {0} state has changed to {1}".format(self._resource_key, state, pilot.uid))

			if state == radical.pilot.FAILED:
				self.get_logger().error("Resource error: ")
				self.get_logger().error("Pattern execution FAILED.")
				self.get_logger().info(pilot.stderr)

			if state == radical.pilot.DONE:
				self.get_logger().info("Resource allocation time over.")
				#self._reporter.info('Resource allocation time over.')

			if state == radical.pilot.CANCELED:
				self.get_logger().info("Resource allocation cancelled.")
				#self._reporter.info('Resource allocation cancelled.')


		self._allocate_called = True
		self.get_logger().info("Allocation process on resource:{0} started".format(self._resource_key))

		# Here we start the pilot(s).
		self.get_logger().info('Ensemble Toolkit (%s)' % version)
		#self.get_logger().report('Starting Allocation')

		# Give priority to mongo url via resource handle
		if  not self._database_url :
			self._database_url = os.getenv ("RADICAL_PILOT_DBURL", None)

		# IF no database url mentioned via environment variable or resource handle, trigger error
		if  not self._database_url :
			raise Exception ("no database URL (set RADICAL_PILOT_DBURL or via resource handle)")  

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
			self.get_logger().info('Created radical pilot manager')
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

			pdesc.access_schema = self._access_schema 

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

			# Wait for pilot to go Active
			if wait is True:
				self.get_logger().info('Waiting for Pilot to go Active')
				#self.get_logger().report('Waiting for Pilot to go Active')
				self._pilot.wait(radical.pilot.ACTIVE)


			# Create unit manager to submit CUs
			self._umgr = radical.pilot.UnitManager( session=self._session, scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION)
			self._umgr.add_pilots(self._pilot)
			self.get_logger().info("Radical pilot unit manager created")

			#self._reporter.ok('>> ok')

		except Exception, ex:
			self.get_logger().error("Fatal error during resource allocation: {0}.".format(str(ex)))
			if self._session:
				self._session.close()
			raise

	#---------------------------------------------------------------------------
	#
	def run(self, appManager):
		'''Runs the workload currently added in the appManager'''

		# Make sure resources were allocated.
		if self._allocate_called is False:
			raise EnTKError(msg="Resource(s) not allocated. Call allocate() first.")

		try:
			appManager.run(resource = self._resource_key, task_manager = self._umgr, rp_session=self._session)

		except Exception, ex:
			self.get_logger().error('Application Manager failed: {0}'.format(ex))
