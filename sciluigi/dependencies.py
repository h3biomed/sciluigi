'''
This module contains functionality for dependency resolution for constructing
the dependency graph of workflows.
'''

import boto3
from collections import Mapping, Sequence
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
        return set([i.target for i in self.target_infos])

    @property
    def target(self):
        if len(self.targets) == 1:
            return self.targets.pop()
        raise ValueError('This TaskInput must be connected to only one TargetInfo in order to access this property')

    @property
    def tasks(self):
        return set([i.task for i in self.target_infos])

    @property
    def task(self):
        if len(self.tasks) == 1:
            return self.tasks.pop()
        raise ValueError('This TaskInput must be connected to only one TargetInfo in order to access this property')

    @property
    def paths(self):
        return list(set([t.path for t in self.target_infos]))

    @property
    def path(self):
        if len(self.paths) == 1:
            return self.paths[0]
        raise ValueError('This TaskInput must be connected to only one TargetInfo in order to access this property')

    def __init__(self):
        self.target_infos = set([])
        self.downstream_inputs = set([])

    def __iter__(self):
        return self.target_infos.__iter__()

    def receive_from(self, connection):
        if isinstance(connection, Sequence):
            for item in connection:
                self.receive_from(item)
        elif isinstance(connection, Mapping):
            for item in connection.values():
                self.receive_from(item)
        elif hasattr(connection, 'target_infos'):
            # If the user tried to connect a TaskInput, connect all of the TaskInput's TargetInfos to self
            # Then add self to the TaskInput's downstream connections
            # Also make sure to connect this connection to any of self's downstream inputs.
            for info in connection.target_infos:
                self.receive_from(info)
            connection.downstream_inputs.add(self)
            for downstream_input in self.downstream_inputs:
                downstream_input.receive_from(connection)
        else:
            # If the user is connecting a TargetInfo, add the TargetInfo to this input and any downstream inputs
            self.target_infos.add(connection)
            for downstream_input in self.downstream_inputs:
                downstream_input.target_infos.add(connection)

    def send_to(self, connection):
        _send(self, connection)


class WorkflowOutput(TaskInput):

    def __init__(self, sub_workflow_task):
        super(WorkflowOutput, self).__init__()


def _send(from_obj, to_obj):
    if not hasattr(to_obj, 'receive_from'):
        raise ValueError('Given connection cannot receive objects')
    to_obj.receive_from(from_obj)


class TargetInfo(object):
    '''
    Class to be used for sending specification of which target, from which
    task, to use, when stitching workflow tasks' outputs and inputs together.
    '''
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

    def send_to(self, connection):
        _send(self, connection)


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

    def open(self, *args, **kwargs):
        '''
        Forward open method, from luigi's target class
        '''
        return self.target.open(*args, **kwargs)


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
                if isinstance(attrval, Mapping):
                    for item in attrval.values():
                        input_attrs.append(item)
                elif isinstance(attrval, Sequence):
                    for item in attrval:
                        input_attrs.append(item)
                else:
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

        if isinstance(val, TaskInput):
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

    def get_output_attrs(self):
        output_attrs = []
        for attrname, attrval in iteritems(self.__dict__):
            if self._is_property(attrname):
                continue  # Properties can't be outputs
            if 'out_' == attrname[0:4]:
                if isinstance(attrval, Mapping):
                    for key in attrval:
                        output_attrs.append(attrval[key])
                elif isinstance(attrval, Sequence):
                    for item in attrval:
                        output_attrs.append(item)
                else:
                    output_attrs.append(attrval)
        return output_attrs

    def _output_targets(self):
        '''
        Extract output targets from the TargetInfo objects
        or functions returning those (or lists of both the earlier)
        for use in luigi's output() method.
        '''
        return [info.target for info in self._output_infos()]

    def _output_infos(self):
        infos = []
        output_attrs = self.get_output_attrs()
        for attrval in output_attrs:
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
        else:
            raise Exception('Input item is neither callable, WorkflowOutput, TargetInfo, nor list: %s' % val)
        return target_infos

    def _is_property(self, attrname):
        if hasattr(type(self), attrname):
            return isinstance(getattr(type(self), attrname), property)
        else:
            return False
