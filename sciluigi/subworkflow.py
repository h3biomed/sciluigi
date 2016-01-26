import logging

log = logging.getLogger('sciluigi-interface')

class SubWorkflowTask(sciluigi.Task):

    def __init__(self, *args, **kwargs):
        super(SubWorkflowTask, self).__init__(*args, **kwargs)
        last_task = self.sub_workflow()
        for attrname in dir(last_task):
            attrval = getattr(last_task, attrname)
            if attrname[0:4] == 'out_':
                setattr(self, attrname, attrval)

    def new_task(self, instance_name, cls, **kwargs):
        instance_name = '%s - %s' % (self.instance_name, instance_name)
        while self.workflow_task._tasks.has_key(instance_name):
            instance_name += ' - Dupe'
        return self.workflow_task.new_task(instance_name, cls, **kwargs)

    def sub_workflow(self):
        raise NotImplementedError