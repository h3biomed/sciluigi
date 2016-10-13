'''
This module contains sciluigi's subclasses of luigi's Task class.
'''

import datetime
import luigi
import logging
import os
import os.path as op
import sciluigi
import sciluigi.audit
import sciluigi.interface
import sciluigi.dependencies
import sciluigi.slurm

log = logging.getLogger('sciluigi-interface')

# ==============================================================================

class WorkflowTask(sciluigi.audit.AuditTrailHelpers, luigi.Task):
    '''
    SciLuigi-specific task, that has a method for implementing a (dynamic) workflow
    definition (workflow()).
    '''

    instance_name = luigi.Parameter(default='sciluigi_workflow')

    _tasks = {}
    _wfstart = ''
    _wflogpath = ''
    _hasloggedstart = False
    _hasloggedfinish = False

    def _ensure_timestamp(self):
        '''
        Make sure that there is a time stamp for when the workflow started.
        '''
        if self._wfstart == '':
            self._wfstart = datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')

    def get_auditdirpath(self):
        '''
        Get the path to the workflow-speicfic audit trail directory.
        '''
        self._ensure_timestamp()
        clsname = self.__class__.__name__.lower()
        audit_dirpath = 'sciluigi-audit/audit_%s_%s' % (clsname, self._wfstart)
        return audit_dirpath

    def get_auditlogpath(self):
        '''
        Get the path to the workflow-speicfic audit trail file.
        '''
        self._ensure_timestamp()
        clsname = self.__class__.__name__.lower()
        audit_dirpath = 'sciluigi-audit/workflow_%s_started_%s.audit' % (clsname, self._wfstart)
        return audit_dirpath

    def add_auditinfo(self, infotype, infolog):
        '''
        Add audit information to the audit log.
        '''
        return self._add_auditinfo(self.__class__.__name__.lower(), infotype, infolog)

    def workflow(self):
        '''
        SciLuigi API methoed. Implement your workflow here, and return the last task(s)
        of the dependency graph.
        '''
        raise WorkflowNotImplementedException(
                'workflow() method is not implemented, for ' + str(self))

    def requires(self):
        '''
        Implementation of Luigi API method.
        '''
        clsname = self.__class__.__name__
        if not self._hasloggedstart:
            log.info('-'*80)
            log.info('SciLuigi: %s Workflow Started', clsname)
            log.info('-'*80)
            self._hasloggedstart = True
        workflow_output = self.workflow()
        if workflow_output is None:
            clsname = self.__class__.__name__
            raise Exception(('Nothing returned from workflow() method in the %s Workflow task. '
                             'Forgot to add a return statement at the end?') % clsname)
        return workflow_output

    def output(self):
        '''
        Implementation of Luigi API method
        '''
        log.info(op.abspath(self.get_auditlogpath()))
        return luigi.LocalTarget(self.get_auditlogpath())

    def run(self):
        '''
        Implementation of Luigi API method
        '''
        log.debug('Running the workflow')
        if self.output().exists():
            errmsg = ('Audit file already exists, '
                      'when trying to create it: %s') % self.output().path
            log.error(errmsg)
            raise Exception(errmsg)
        else:
            with self.output().open('w') as auditfile:
                for taskname in sorted(self._tasks):
                    taskaudit_path = os.path.join(self.get_auditdirpath(), taskname)
                    if os.path.exists(taskaudit_path):
                        auditfile.write(open(taskaudit_path).read() + '\n')
        clsname = self.__class__.__name__
        if not self._hasloggedfinish:
            log.info('-'*80)
            log.info('SciLuigi: %s Workflow Finished', clsname)
            log.info('-'*80)
            self._hasloggedfinish = True

    def new_task(self, instance_name, cls, **kwargs):
        '''
        Create new task instance, and link it to the current workflow.
        '''
        newtask = sciluigi.new_task(instance_name, cls, self, **kwargs)
        self._tasks[instance_name] = newtask
        return newtask

# ================================================================================

class WorkflowNotImplementedException(Exception):
    '''
    Exception to throw if the workflow() SciLuigi API method is not implemented.
    '''
    pass
