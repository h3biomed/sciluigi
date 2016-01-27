import logging
import sciluigi

log = logging.getLogger('sciluigi-interface')


class SubWorkflowTask(sciluigi.task.Task):

    _final_tasks = []

    @property
    def final_tasks(self):
        return self._final_tasks

    @final_tasks.setter
    def final_tasks(self, value):
        # Force final_tasks to be a list
        if not isinstance(value, list):
            self._final_tasks = [value]
        else:
            self._final_tasks = value

    def _call_sub_workflow(self):
        return_val = self.sub_workflow()
        self.final_tasks = return_val
        return return_val

    def new_task(self, instance_name, cls, **kwargs):
        instance_name = '%s - %s' % (self.instance_name, instance_name)
        while self.workflow_task._tasks.has_key(instance_name):
            instance_name += ' - Dupe'
        return self.workflow_task.new_task(instance_name, cls, **kwargs)

    def sub_workflow(self):
        raise NotImplementedError
        
    def requires(self):
        yield self._call_sub_workflow()
        yield super(SubWorkflowTask, self).requires()