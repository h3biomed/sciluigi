'''
This module contains sciluigi's subclasses of luigi's Task class.
'''

import copy
import luigi
import logging
import subprocess as sub
import sciluigi.interface
import sciluigi.dependencies
import sciluigi.slurm

log = logging.getLogger('sciluigi-interface')

# ==============================================================================


def new_task(name, cls, workflow_properties, **kwargs):
    slurminfo = None
    if 'sciluigi_reduce_function' not in kwargs:
        kwargs['sciluigi_reduce_args'] = (name, cls, copy.deepcopy(kwargs))
        kwargs['sciluigi_reduce_function'] = _new_task_unpickle
    kwargs['instance_name'] = name
    kwargs['workflow_properties'] = workflow_properties
    newtask = cls(**kwargs)
    if slurminfo is not None:
        newtask.slurminfo = slurminfo
    return newtask


def _new_task_unpickle(instance_name, cls, kwargs):
    # Make sure the workflow has been initialized before any other unpickling is done
    kwargs['sciluigi_unpickling'] = True
    return new_task(instance_name, cls, **kwargs)


class MetaTask(luigi.task_register.Register):
    def __call__(cls, *args, **kwargs):
        # Allows us to pass in properties that aren't Luigi params
        sciluigi_reduce_function = kwargs.pop('sciluigi_reduce_function', None)
        sciluigi_reduce_args = kwargs.pop('sciluigi_reduce_args', None)
        workflow_properties = kwargs.pop('workflow_properties', None)

        new_instance = super(MetaTask, cls).__call__(*args, **kwargs)
        new_instance.sciluigi_reduce_args = sciluigi_reduce_args
        new_instance.sciluigi_reduce_function = sciluigi_reduce_function
        new_instance.sciluigi_state = new_instance.__dict__
        new_instance.workflow_properties = workflow_properties
        return new_instance


class Task(sciluigi.dependencies.DependencyHelpers, luigi.Task):
    '''
    SciLuigi Task, implementing SciLuigi specific functionality for dependency resolution
    and audit trail logging.
    '''
    __metaclass__ = MetaTask

    workflow_properties = luigi.Parameter(significant=False)
    instance_name = luigi.Parameter(significant=False)
    sciluigi_unpickling = luigi.Parameter(default=False, significant=False)

    def __deepcopy__(self, memo):
        return self

    def __reduce__(self):
        return self.sciluigi_reduce_function, self.sciluigi_reduce_args, self.sciluigi_state

    def __init__(self, *args, **kwargs):
        super(Task, self).__init__(*args, **kwargs)
        if not self.sciluigi_unpickling:
            self.initialize_inputs_and_outputs()

    def initialize_inputs_and_outputs(self):
        raise NotImplementedError

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
            if isinstance(output, sciluigi.S3TargetInfo):
                output.target.fs.put_string('', output.path)
            else:
                task.ex_local('touch ' + output.path)

# ==============================================================================

class ExternalTask(sciluigi.dependencies.DependencyHelpers, luigi.ExternalTask):
    '''
    SviLuigi specific implementation of luigi.ExternalTask, representing existing
    files.
    '''
    workflow_properties = luigi.Parameter(significant=False)
    instance_name = luigi.Parameter(significant=False)

    def __init__(self, *args, **kwargs):
        super(ExternalTask, self).__init__(*args, **kwargs)
        self.initialize_inputs_and_outputs()

    def initialize_input_and_outputs(self):
        raise NotImplementedError
