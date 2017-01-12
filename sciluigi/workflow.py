'''
This module contains sciluigi's subclasses of luigi's Task class.
'''

import datetime
import luigi
from luigi.six import iteritems
import logging
import os
import os.path as op
import random
from subprocess import check_call
import time

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

    def get_output_path(self):
        '''
        Get the path to the workflow-speicfic audit trail file.
        '''
        self._ensure_timestamp()
        clsname = self.__class__.__name__.lower()
        output_dirpath = 'sciluigi-output/workflow_%s_started_%s.out' % (clsname, self._wfstart)
        return output_dirpath

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
        return luigi.LocalTarget(self.get_output_path())

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

    @luigi.Task.event_handler(luigi.Event.PROCESSING_TIME)
    def save_end_time(self, task_exectime_sec):
        '''
        Log end of execution of task, with execution time.
        '''
        msg = 'Workflow {task} finished after {proctime:.3f}s'.format(
            task=self.get_instance_name(),
            proctime=task_exectime_sec)
        log.info(msg)
        self._add_output_info(self.__class__.__name__.lower(), 'task_exectime_sec', '%.3f' % task_exectime_sec)
        for paramname, paramval in iteritems(self.param_kwargs):
            if paramname not in ['workflow_task']:
                self.add_auditinfo(paramname, paramval)

    def _add_output_info(self, instance_name, infotype, infoval):
        '''
        Save audit information in a designated file, specific for this task.
        '''
        dirpath = self.get_output_path()
        if not op.isdir(dirpath):
            time.sleep(random.random())
            if not os.path.isdir(dirpath):
                os.makedirs(dirpath)

        output_file = os.path.join(dirpath, instance_name)
        if not os.path.exists(output_file):
            with open(output_file, 'w') as afile:
                afile.write('[%s]\n' % self.instance_name)
        with open(output_file, 'a') as afile:
            afile.write('%s: %s\n' % (infotype, infoval))

# ================================================================================

class WorkflowNotImplementedException(Exception):
    '''
    Exception to throw if the workflow() SciLuigi API method is not implemented.
    '''
    pass
