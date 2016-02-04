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
        while self.workflow_task._tasks.has_key(instance_name):
            instance_name += ' - Dupe'
        return self.workflow_task.new_task(instance_name, cls, **kwargs)

    def sub_workflow(self):
        raise NotImplementedError

    def requires(self):
        requirements = [super(SubWorkflowTask, self).requires()]
        outputs = luigi.task.flatten(self.output())
        for output in outputs:
            requirements.append(output.task)
        return requirements
