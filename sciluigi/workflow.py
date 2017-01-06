'''
This module contains sciluigi's subclasses of luigi's Task class.
'''

import datetime
import luigi
import logging
import sciluigi
import sciluigi.interface
import sciluigi.dependencies
import sciluigi.slurm
from subprocess import check_call

log = logging.getLogger('sciluigi-interface')


# ==============================================================================

class WorkflowTask(luigi.Task):
    '''
    SciLuigi-specific task, that has a method for implementing a (dynamic) workflow
    definition (workflow()).
    '''

    instance_name = luigi.Parameter(default='sciluigi_workflow')

    def __init__(self, *args, **kwargs):
        super(WorkflowTask, self).__init__(*args, **kwargs)
        self._tasks = {}
        self._wfstart = ''
        self._wflogpath = ''
        self._hasloggedstart = False
        self._hasloggedfinish = False

    def __deepcopy__(self, memo):
        return self

    def _ensure_timestamp(self):
        '''
        Make sure that there is a time stamp for when the workflow started.
        '''
        if self._wfstart == '':
            self._wfstart = datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')

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
        return luigi.LocalTarget('sciluigi/_SUCCESS')

    def run(self):
        '''
        Implementation of Luigi API method
        '''
        log.debug('Running the workflow')
        if self.output().exists():
            errmsg = ('Success file already exists, '
                      'when trying to create it: %s') % self.output().path
            log.error(errmsg)
            raise Exception(errmsg)
        else:
            check_call(['touch', self.output().path])
        clsname = self.__class__.__name__
        if not self._hasloggedfinish:
            log.info('-'*80)
            log.info('SciLuigi: %s Workflow Finished', clsname)
            log.info('-'*80)
            self._hasloggedfinish = True

# ================================================================================

class WorkflowNotImplementedException(Exception):
    '''
    Exception to throw if the workflow() SciLuigi API method is not implemented.
    '''
    pass
