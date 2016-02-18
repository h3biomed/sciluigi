'''
This module contains functionality for dependency resolution for constructing
the dependency graph of workflows.
'''

import boto3
import luigi
from luigi.s3 import S3Client, S3Target
from luigi.six import iteritems
import os


# ==============================================================================

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

    def _upstream_tasks(self):
        '''
        Extract upstream tasks from the TargetInfo objects
        or functions returning those (or lists of both the earlier)
        for use in luigi's requires() method.
        '''
        upstream_tasks = []
        for input_name, input_val in iteritems(self.inputs):
            upstream_tasks = self._parse_inputitem(input_val, upstream_tasks)

        return upstream_tasks

    def _parse_inputitem(self, val, tasks):
        '''
        Recursively loop through lists of TargetInfos, or
        callables returning TargetInfos, or lists of ...
        (repeat recursively) ... and return all tasks.
        '''
        if callable(val):
            val = val()
        if isinstance(val, TargetInfo):
            tasks.append(val.task)
        elif isinstance(val, list):
            for valitem in val:
                tasks = self._parse_inputitem(valitem, tasks)
        elif isinstance(val, dict):
            for _, valitem in iteritems(val):
                tasks = self._parse_inputitem(valitem, tasks)
        else:
            raise Exception('Input item is neither callable, TargetInfo, nor list: %s' % val)
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
        for output_name, output_val in iteritems(self.outputs):
            infos = self._parse_outputitem(output_val, infos)
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
