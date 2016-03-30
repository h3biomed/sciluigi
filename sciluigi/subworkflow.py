import logging
import luigi
import sciluigi

log = logging.getLogger('sciluigi-interface')


class SubWorkflowTask(sciluigi.task.Task):

    def new_task(self, instance_name, cls, **kwargs):
        instance_name = '%s - %s' % (self.instance_name, instance_name)
        return self.workflow_task.new_task(instance_name, cls, **kwargs)

    def initialize_tasks(self):
        raise NotImplementedError

    def initialize_inputs_and_outputs(self):
        # Sneaks initialize_tasks() into the Task constructor before initialize_inputs_and_outputs gets called
        self.initialize_tasks()
        super(SubWorkflowTask, self).initialize_inputs_and_outputs()

    def connect_tasks(self):
        raise NotImplementedError

    def requires(self):
        endpoints = [self.connect_tasks()]
        requirements = [super(SubWorkflowTask, self).requires()]
        return endpoints + requirements
