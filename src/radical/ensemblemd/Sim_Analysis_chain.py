#!/usr/bin/env python

from radical.ensemblemd.task import Task
from radical.ensemblemd.batch import Batch
from radical.ensemblemd.exceptions import TypeError
from radical.ensemblemd.execution_pattern import ExecutionPattern

PATTERN_NAME = "SimAnalysisChain"

class SimAnalysisChain(ExecutionPattern):
    
  def __init__(self, stages=[]):
    """Creates a new Task instance."""
    super(Pipeline, self).__init__()
    # self._steps contains the list of tasks in this pipeline.
    self._stages = list()
    self.add_stages(stages)
    
  def get_name(self):
    """Implements base class ExecutionPattern.get_name()"""
    return PATTERN_NAME
  
  def _get_pattern_workload(self):
  """Returns a structured description of the tasks in the pipeline"""
    return self._stages
    
  def add_stages(self,stages):
    """Implements base class ExecutionPattern.get_name()"""
    
    if type(steps) != list:
      raise TypeError(expected_type=list,actual_type=type(steps))
      
    for stage in stages:
      if type(stage) != Task and type(stage) != Batch:
        raise TypeError(expected_type=[Task, Batch],actual_type=type(stage))
      
      else:
        self._stages.append(stage)
        
        
'''
Usage :


'''
