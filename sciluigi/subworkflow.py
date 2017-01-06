import copy
import logging
import sciluigi

log = logging.getLogger('sciluigi-interface')


class SubWorkflowTask(sciluigi.task.Task):

    def __init__(self, *args, **kwargs):
        super(sciluigi.task.Task, self).__init__(*args, **kwargs)
        if not self.sciluigi_unpickling:
            self.initialize_tasks()
            self.initialize_inputs_and_outputs()
            self.connect_tasks()

    def initialize_tasks(self):
        raise NotImplementedError

    def connect_tasks(self):
        raise NotImplementedError

    def requires(self):
        log.info('Getting sub-workflow requirements for ' + self.__class__.__name__)
        #return self.endpoints

        return [output.sub_workflow_reqs for output in self.get_output_attrs()]
