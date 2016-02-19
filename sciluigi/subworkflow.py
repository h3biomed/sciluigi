import logging
import luigi
import sciluigi

log = logging.getLogger('sciluigi-interface')


class SubWorkflowTask(sciluigi.task.Task):

    def __init__(self, *args, **kwargs):
        super(SubWorkflowTask, self).__init__(*args, **kwargs)
        self.sub_workflow()

    def new_task(self, instance_name, cls, **kwargs):
        instance_name = '%s - %s' % (self.instance_name, instance_name)
        return self.workflow_task.new_task(instance_name, cls, **kwargs)

    def sub_workflow(self):
        raise NotImplementedError

    def requires(self):
        requirements = [super(SubWorkflowTask, self).requires()]
        for output in self.output_infos():
            requirements.append(output.task)
        return requirements
