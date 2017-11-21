import logging

import luigi
import sciluigi
from luigi.six import iteritems

log = logging.getLogger('sciluigi-interface')


class WorkflowTask(sciluigi.task.Task):
    instance_name = luigi.Parameter(default='sciluigi_workflow')

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

    def mirror_outputs(self, inner_workflow, element_id=None):
        for attr_name, attr_val in iteritems(inner_workflow.__dict__):
            if attr_name.startswith('out_'):
                if element_id is not None:
                    setattr(self, '%s-%s-%s' % (attr_name, element_id, inner_workflow.instance_name), attr_val)
                else:
                    setattr(self, '%s-%s' % (attr_name, inner_workflow.instance_name), attr_val)

    def get_all_outputs(self):
        """
        Retrieve a list of all task outputs (i.e. those that start with 'out_')
        :return: a list of all task outputs
        """
        return [attr_val for attr_name, attr_val in iteritems(self.__dict__) if attr_name.startswith('out_')]
