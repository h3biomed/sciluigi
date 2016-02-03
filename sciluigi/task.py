'''
This module contains sciluigi's subclasses of luigi's Task class.
'''

import luigi
from luigi.six import iteritems, string_types
import logging
import subprocess as sub
import sciluigi.audit
import sciluigi.interface
import sciluigi.dependencies
import sciluigi.slurm

log = logging.getLogger('sciluigi-interface')

# ==============================================================================


def new_task(name, cls, workflow_task, **kwargs):
    '''
    Instantiate a new task. Not supposed to be used by the end-user
    (use WorkflowTask.new_task() instead).
    '''
    slurminfo = None
    kwargs['instance_name'] = name
    kwargs['workflow_task'] = workflow_task
    newtask = cls(**kwargs)
    if slurminfo is not None:
        newtask.slurminfo = slurminfo
    return newtask


def task_input(is_optional):
    def wrapped(func):
        func.is_input = True
        func.default_input_value = func(None)
        func.is_optional_input = is_optional
        return func
    return wrapped


def swf_input(is_optional):
    return task_input(is_optional)


def task_output():
    def wrapped(func):
        func.is_output = True
        func.default_output_value = func(None)
        func.output_type = 'task'
        return func
    return wrapped


def swf_output():
    def wrapped(func):
        func.is_output = True
        func.default_output_value = func(None)
        func.output_type = 'swf'
        return func
    return wrapped


def generate_getter(label, default_value):
    private_label = '_' + label

    def return_func(self_arg):
        try:
            return getattr(self_arg, private_label)
        except AttributeError:
            return default_value

    return return_func


def generate_input_setter(label, is_optional):
    private_label = '_' + label

    def _parse_target(target):
        if isinstance(target, sciluigi.TargetInfo):
            if target.is_empty():
                if is_optional:
                    raise ValueError('Cannot link empty target to non-optional input %s' % label)
                else:
                    return None
            else:
                return target
        elif isinstance(target, dict):
            parsed_target = {}
            for key in target:
                parsed_target[key] = _parse_target(target[key])
            return parsed_target
        elif isinstance(target, list):
            parsed_target = []
            for individual_target in target:
                parsed_target.append(_parse_target(individual_target))
            return parsed_target
        else:
            raise ValueError('Invalid value type. Must be TargetInfo, dict, or list')

    def return_func(self_arg, val):
        setattr(self_arg, private_label, _parse_target(val))

    return return_func


def generate_output_setter(label, output_type):
    private_label = '_' + label

    def return_func(self_arg, val):
        setattr(self_arg, private_label, val)

    if output_type == 'task':
        return None
    elif output_type == 'swf':
        return return_func
    else:
        raise ValueError('Invalid output type')


class TaskMeta(type):

    def __new__(mcs, clsname, bases, attrs):
        for name, method in attrs.iteritems():
            if hasattr(method, 'is_input'):
                attrs[name] = property(fget=generate_getter(name, method.default_input_value),
                                       fset=generate_input_setter(name, method.is_optional_input))
            elif hasattr(method, 'is_output'):
                attrs[name] = property(fget=generate_getter(name, method.default_output_value_value),
                                       fset=generate_output_setter(name, method.output_type))
        return super(TaskMeta, mcs).__new__(mcs, clsname, bases, attrs)


class Task(sciluigi.audit.AuditTrailHelpers, sciluigi.dependencies.DependencyHelpers, luigi.Task):
    '''
    SciLuigi Task, implementing SciLuigi specific functionality for dependency resolution
    and audit trail logging.
    '''

    __metaclass__ = TaskMeta

    workflow_task = luigi.Parameter(significant=False)
    instance_name = luigi.Parameter(significant=False)

    def ex_local(self, command):
        '''
        Execute command locally (not through resource manager).
        '''
        # If list, convert to string
        if isinstance(command, list):
            command = sub.list2cmdline(command)

        log.info('Executing command: ' + str(command))
        proc = sub.Popen(command, shell=True, stdout=sub.PIPE, stderr=sub.PIPE)
        stdout, stderr = proc.communicate()
        retcode = proc.returncode

        if len(stderr) > 0:
            log.debug('Stderr from command: %s', stderr)

        if retcode != 0:
            errmsg = ('Command failed (retcode {ret}): {cmd}\n'
                      'Command output: {out}\n'
                      'Command stderr: {err}').format(
                    ret=retcode,
                    cmd=command,
                    out=stdout,
                    err=stderr)
            log.error(errmsg)
            raise Exception(errmsg)

        return (retcode, stdout, stderr)

    def ex(self, command):
        '''
        Execute command. This is a short-hand function, to be overridden e.g. if supporting
        execution via SLURM
        '''
        return self.ex_local(command)


@Task.event_handler(luigi.Event.SUCCESS)
def touch_unfulfilled_optional(task):
    # If an output is optional, touch it if it does not exist so that no errors will be thrown
    for output in luigi.task.flatten(task.output_infos()):
        if output.is_optional and not output.target.exists():
            task.ex_local('touch ' + output.path)

# ==============================================================================

class ExternalTask(
        sciluigi.audit.AuditTrailHelpers,
        sciluigi.dependencies.DependencyHelpers,
        luigi.ExternalTask):
    '''
    SviLuigi specific implementation of luigi.ExternalTask, representing existing
    files.
    '''
    workflow_task = luigi.Parameter()
    instance_name = luigi.Parameter()
