import copy
import logging
import sciluigi

log = logging.getLogger('sciluigi-interface')


class SubWorkflowTask(sciluigi.task.Task):

    def configure_instance(self):
        self.initialize_tasks()
        self.initialize_inputs_and_outputs()
        self.connect_tasks()

    def initialize_tasks(self):
        raise NotImplementedError

    def connect_tasks(self):
        raise NotImplementedError

    def requires(self):
        return [info.task for info in self.output_infos()]
