import logging
import luigi
from luigi.six import iteritems
import sciluigi

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
        # if isinstance(attrval, Mapping):
        #     for key in attrval:
        #         getattr(self, attrname)[key].receive_from(getattr(inner_workflow, attrname)[key])
        # elif isinstance(attrval, Sequence):
        #     for item in attrval:
        #         get
        # getattr(self, attrname).receive_from(getattr(inner_workflow, attrname))

        for attrname, attrval in iteritems(inner_workflow.__dict__):
            if 'out_' == attrname[0:4]:
                if element_id is not None:
                    setattr(self, '%s-%s-%s' % (attrname, element_id, inner_workflow.instance_name), attrval)
                else:
                    setattr(self, '%s-%s' % (attrname, inner_workflow.instance_name), attrval)
