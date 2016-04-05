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
    def target(self):
        if len(self.target_infos) == 1:
            return self.target_infos[0].target
        raise ValueError('This TaskInput is connected to more than one TargetInfo')

    @property
    def task(self):
        if len(self.target_infos) == 1:
            return self.target_infos[0].task
        raise ValueError('This TaskInput is connected to more than one TargetInfo')

    def __init__(self):
        self.target_infos = []

    def connect(self, target_info):
        if hasattr(target_info, 'target_infos'):
            # If the user tried to connect a TaskInput, just connect all of the TaskInput's TargetInfos
            for info in target_info.target_infos:
                self.connect(info)
        else:
            self.target_infos.append(target_info)

    def disconnect(self, target_info):
        self.target_infos.remove(target_info)


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
        log.info('Getting requirements for ' + self.__class__.__name__)
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
        if isinstance(val, TaskInput):
            tasks += val.tasks
        else:
            raise Exception('Input item is neither callable, TaskInput, nor list: %s' % val)
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
        elif isinstance(val, list):
            for valitem in val:
                target_infos = self._parse_outputitem(valitem, target_infos)
        elif isinstance(val, dict):
            for _, valitem in iteritems(val):
                target_infos = self._parse_outputitem(valitem, target_infos)
        else:
            raise Exception('Input item is neither callable, TargetInfo, nor list: %s' % val)
        return target_infos

    def _is_property(self, attrname):
        if hasattr(type(self), attrname):
            return isinstance(getattr(type(self), attrname), property)
        else:
            return False
