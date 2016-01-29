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
import warnings

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

class Task(sciluigi.audit.AuditTrailHelpers, sciluigi.dependencies.DependencyHelpers, luigi.Task):
    '''
    SciLuigi Task, implementing SciLuigi specific functionality for dependency resolution
    and audit trail logging.
    '''
    workflow_task = luigi.Parameter()
    instance_name = luigi.Parameter()

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

    def complete(self):
        outputs = luigi.task.flatten(self.output_infos())
        if len(outputs) == 0:
            warnings.warn(
                "Task %r without outputs has no custom complete() method" % self,
                stacklevel=2
            )
            return False

        # If an output is optional, touch it if it does not exist so that this method still returns True
        for output in outputs:
            if output.is_optional and not output.target.exists():
                self.ex_local('touch ' + output.path)

        # Return true if all outputs exist
        return all(map(lambda output: output.target.exists(), outputs))

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
