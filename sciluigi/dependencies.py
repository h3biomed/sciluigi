'''
This module contains functionality for dependency resolution for constructing
the dependency graph of workflows.
'''

import boto3
import luigi
from luigi.s3 import S3Client, S3Target
from luigi.six import iteritems
import os

import logging

log = logging.getLogger('sciluigi-interface')


# ==============================================================================

class TaskInput(object):

    @property
    def targets(self):
        return [i.target for i in self.target_infos]

    @property
    def tasks(self):
        return [i.task for i in self.target_infos]

    @property
    def path(self):
        if len(self.target_infos) == 1:
            return [t.path for t in self.target_infos][0]
        raise ValueError('This TaskInput is connected to more than one TargetInfo')

    @property
    def target(self):
        if len(self.target_infos) == 1:
            return [t.target for t in self.target_infos][0]
        raise ValueError('This TaskInput is connected to more than one TargetInfo')

    @property
    def task(self):
        if len(self.target_infos) == 1:
            return [t.task for t in self.target_infos][0]
        raise ValueError('This TaskInput is connected to more than one TargetInfo')

    def __init__(self):
        self.target_infos = set([])
        self.downstream_inputs = set([])

    def __iter__(self):
        return self.target_infos.__iter__()

    def connect(self, connection):
        if isinstance(connection, list):
            for item in connection:
                self.connect(item)
        elif hasattr(connection, 'target_infos'):
            # If the user tried to connect a TaskInput, first check if it's a SubWorkflowOutput.  If so, just connect,
            # the SubWorkflow.  Otherwise, connect all of the TaskInput's TargetInfos to self.
            # Then add self to the TaskInput's downstream connections
            if hasattr(connection, 'sub_workflow_task'):
                self.sub_workflow_task = connection.sub_workflow_task  # Make note of sub_workflow_task if applicable
            else:
                for info in connection.target_infos:
                    self.connect(info)
                connection.downstream_inputs.add(self)
        else:
            # If the user is connecting a TargetInfo, add the TargetInfo to this input and any downstream inputs
            self.target_infos.add(connection)
            for downstream_input in self.downstream_inputs:
                downstream_input.target_infos.add(connection)

    def disconnect(self, target_info):
        self.target_infos.remove(target_info)


class SubWorkflowOutput(TaskInput):

    def __init__(self, sub_workflow_task):
        super(SubWorkflowOutput, self).__init__()
        self.sub_workflow_task = sub_workflow_task

    def connect(self, connection):
        if hasattr(connection, 'target_infos'):
            raise Exception('You can only connect TargetInfo objects to a SubWorkflowOuput')

        if isinstance(connection, list):
            for item in connection:
                self.connect(item)
        else:
            super(SubWorkflowOutput, self).connect(connection)


class TargetInfo(object):
    '''
    Class to be used for sending specification of which target, from which
    task, to use, when stitching workflow tasks' outputs and inputs together.
    '''
    task = None
    path = None
    target = None

    def __init__(self, task, path, format=None, is_optional=False, is_tmp=False):
        self.task = task
        self.path = path
        self.is_optional = is_optional
        self.target = luigi.LocalTarget(path, format, is_tmp)

    def is_empty(self):
        return os.stat(self.path).st_size == 0

    def open(self, *args, **kwargs):
        '''
        Forward open method, from luigi's target class
        '''
        return self.target.open(*args, **kwargs)


# ==============================================================================

class S3TargetInfo(TargetInfo):
    def __init__(self, task, path, is_optional=False, format=None, client=None):
        self.task = task
        self.path = path
        self.is_optional = is_optional
        self.target = S3Target(path, format=format, client=client)

    @property
    def is_empty(self):
        s3 = boto3.resource('s3')
        (bucket, key) = S3Client._path_to_bucket_and_key(self.path)
        return s3.ObjectSummary(bucket, key).size == 0


# ==============================================================================

class DependencyHelpers(object):
    '''
    Mixin implementing methods for supporting dynamic, and target-based
    workflow definition, as opposed to the task-based one in vanilla luigi.
    '''

    # --------------------------------------------------------
    # Handle inputs
    # --------------------------------------------------------
    def requires(self):
        '''
        Implement luigi API method by returning upstream tasks
        '''
        return self._upstream_tasks()
        
    def get_input_attrs(self):
        input_attrs = []
        for attrname, attrval in iteritems(self.__dict__):
            if 'in_' == attrname[0:3]:
                input_attrs.append(attrval)
        return input_attrs

    def _upstream_tasks(self):
        '''
        Extract upstream tasks from the TargetInfo objects
        or functions returning those (or lists of both the earlier)
        for use in luigi's requires() method.
        '''
        upstream_tasks = []
        for attrval in self.get_input_attrs():
            upstream_tasks = self._parse_inputitem(attrval, upstream_tasks)

        return upstream_tasks

    def _parse_inputitem(self, val, tasks):
        '''
        Recursively loop through lists of TargetInfos, or
        callables returning TargetInfos, or lists of ...
        (repeat recursively) ... and return all tasks.
        '''
        if callable(val):
            val = val()

        if hasattr(val, 'sub_workflow_task'):
            tasks.append(val.sub_workflow_task)
        elif isinstance(val, TaskInput):
            tasks += val.tasks
        else:
            raise Exception('Input item is neither callable nor a TaskInput: %s' % val)
        return tasks

    # --------------------------------------------------------
    # Handle outputs
    # --------------------------------------------------------

    def output(self):
        '''
        Implement luigi API method
        '''
        return self._output_targets()

    def output_infos(self):
        return self._output_infos()

    def _output_targets(self):
        '''
        Extract output targets from the TargetInfo objects
        or functions returning those (or lists of both the earlier)
        for use in luigi's output() method.
        '''
        return [info.target for info in self._output_infos()]

    def _output_infos(self):
        infos = []
        for attrname in dir(self):
            if self._is_property(attrname):
                continue # Properties can't be outputs
            attrval = getattr(self, attrname)
            if attrname[0:4] == 'out_':
                infos = self._parse_outputitem(attrval, infos)

        return infos

    def _parse_outputitem(self, val, target_infos):
        '''
        Recursively loop through lists of TargetInfos, or
        callables returning TargetInfos, or lists of ...
        (repeat recursively) ... and return all targets.
        '''
        if callable(val):
            val = val()
        if isinstance(val, TargetInfo):
            target_infos.append(val)
        elif isinstance (val, TaskInput):
            for info in val:
                target_infos.append(info)
        elif isinstance(val, list):
            for valitem in val:
                target_infos = self._parse_outputitem(valitem, target_infos)
        elif isinstance(val, dict):
            for _, valitem in iteritems(val):
                target_infos = self._parse_outputitem(valitem, target_infos)
        else:
            raise Exception('Input item is neither callable, SubWorkflowOutput, TargetInfo, nor list: %s' % val)
        return target_infos

    def _is_property(self, attrname):
        if hasattr(type(self), attrname):
            return isinstance(getattr(type(self), attrname), property)
        else:
            return False
