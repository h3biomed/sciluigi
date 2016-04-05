import logging
import sciluigi

log = logging.getLogger('sciluigi-interface')


class SubWorkflowTask(sciluigi.task.Task):

    def __init__(self, *args, **kwargs):
        super(sciluigi.task.Task, self).__init__(*args, **kwargs)
        log.info('Initializing tasks for ' + self.__class__.__name__)
        self.initialize_tasks()
        log.info('Initializing inputs and outputs for ' + self.__class__.__name__)
        self.initialize_inputs_and_outputs()
        log.info('Connecting tasks for ' + self.__class__.__name__)
        self.endpoints = [self.connect_tasks()]

    def initialize_tasks(self):
        raise NotImplementedError

    def new_task(self, instance_name, cls, **kwargs):
        instance_name = '%s - %s' % (self.instance_name, instance_name)
        return self.workflow_task.new_task(instance_name, cls, **kwargs)

    def connect_tasks(self):
        raise NotImplementedError

    def requires(self):
        log.info('Getting sub-workflow requirements for ' + self.__class__.__name__)
        #requirements = [super(SubWorkflowTask, self).requires()]
        return self.endpoints  #+ requirements
