import logging
import sciluigi

log = logging.getLogger('sciluigi-interface')

class SubWorkflowTask(sciluigi.Task):

    @property
    def final_tasks(self):
        swf = self.sub_workflow()
        try:
            _ = (t for t in swf)
        except TypeError:
            return [swf]
        return swf

    def new_task(self, instance_name, cls, **kwargs):
        instance_name = '%s - %s' % (self.instance_name, instance_name)
        while self.workflow_task._tasks.has_key(instance_name):
            instance_name += ' - Dupe'
        return self.workflow_task.new_task(instance_name, cls, **kwargs)

    def sub_workflow(self):
        raise NotImplementedError
        
    def requires(self):
        yield self.sub_workflow()
        yield super(SubWorkflowTask, self).requires()