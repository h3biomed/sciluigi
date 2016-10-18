Sub-Workflows
==============

.. _sub_workflow_overview:

Overview
--------

A sub-workflow is like a mini :doc:`Workflow <workflows>` that you can use as a component of a larger workflow, as you
would a :doc:`Task <tasks>`.  The main difference between sub-workflows and workflows is that a sub-workflow can have
inputs and outputs, just like a task.  Sub-workflows can therefore "package" several tasks together in a reusable
mini-workflow that can then be injected into any workflow.

The image below shows the concept of a sub-workflow from the perspective of the workflow.  In essence, it behaves just
like a task.  It's a piece of a larger pipeline that has inputs that are fed to it be Task A and outputs which are fed
to Task D.

.. figure:: SubWorkflow.png

However, the sub-workflow really consists of multiple tasks, as shown in the next image.  The sub-workflow is a
mini-workflow where Task B passes its output into the input of Task C.  It gets a little tricky because there is the
need to differentiate between sub-workflow inputs/outputs and task inputs/outputs.  The sub-workflow inputs and outputs
are indicated with the purple arrows, while the task inputs and outputs are indicated with the dark blue arrows.  Task
B requires an input, and it gets that input from the input of the sub-workflow as a whole.  Similarly, Task C produces
an output, which it passes on to the output of the sub-workflow as a whole.  It is this process of connecting the
inputs and outputs of the tasks within a sub-workflow to the inputs and outputs of the sub-workflow itself that allows
the parent workflow to treat the sub-workflow as an individual task.

.. figure:: FullSubWorkflow.png

Declaring a Sub-Workflow
-------------------------

The first step is to declare your sub-workflow class.  A sub-workflow should inherit from the
:class:`~sciluigi.subworkflow.SubWorkflowTask` class.  Like a regular :doc:`SciLuigi task <tasks>`, a workflow can
accept Luigi parameters.

Code:

.. code-block:: python

    import luigi
    from sciluigi import SubWorkflowTask

    class MySubWorkflow(SubWorkflowTask):

        my_param = luigi.Parameter()
        my_other_param = luigi.Parameter()

        # Rest of sub-workflow code goes here

Initializing Tasks
------------------

The first step is to initialize all of the tasks that will make up your sub-workflow.  To do this, you'll need to
implement the :meth:`~sciluigi.subworkflow.SubWorkflowTask.initialize_tasks` method.  Within this method, you simply
declare tasks in the :doc:`same way as you do within a Workflow's workflow method <workflows>`.  See example below.

.. code-block:: python

    def initialize_tasks(self):
        self.my_first_task = self.new_task('My Task Name', MyTask, task_param='foo', another_task_param='bar')
        self.my_second_task = self.new_task('My Other Task Name', MyOtherTask, yet_another_task_param='baz')
        self.my_third_task = self.new_task('My Third Task Name', MyThirdTask)

Declaring Inputs and Outputs
-----------------------------

Once your tasks have been initialized, you need to declare your inputs and outputs.  You must do this within an
:meth:`~sciluigi.task.Task.initialize_inputs_and_outputs` method.  The format for this declaration is
:ref:`nearly the same as for the input/output declaration of a task <task_inputs_outputs>`.  The big difference is
that, instead of assigning a :class:`~sciluigi.dependencies.TargetInfo` or an
:class:`~sciluigi.dependencies.S3TargetInfo` to the outputs, you assign a
:class:`~sciluigi.dependencies.SubWorkflowOutput`.  You must do this because the outputs of a sub-workflow will
actually be outputs of one or more tasks within the sub-workflow (see the example in the
:ref:`overview <sub_workflow_overview>` where the sub-workflow output was actually connected to the output of Task C).
**As with tasks, all input names must begin with** ``in_`` **and all output names must begin with** ``out_``.  See the
example below.

.. code-block:: python

    from sciluigi import TaskInput
    from sciluigi import SubWorkflowOutput

    def initialize_inputs_and_outputs(self):
        self.in_my_input = TaskInput()

        self.out_my_output = SubWorkflowOutput(self)

Connecting Tasks, Inputs, and Outputs
--------------------------------------

Finally, you must connect your tasks, inputs, and outputs together.  This must be done in a separate
:meth:`~sciluigi.subworkflow.SubWorkflowTask.connect_tasks` method.  Connecting tasks is essentially the same within a
sub-workflow as :ref:`within a workflow <connect_tasks>`.  The main difference is that you must also hook up the
sub-workflow's inputs and outputs.  See the example below.

.. code-block:: python

    def connect_tasks(self):
            # Connecting the sub-workflow input to my_first_task.in_some_input
            self.my_first_task.in_some_input.connect(self.in_my_input)

            self.my_second_task.in_some_other_input.connect(self.my_first_task.out_some_output)

            self.my_third_task.in_input_1.connect(self.my_second_task.out_output_1)
            self.my_third_task.in_input_2.connect(self.my_second_task.out_output2)

            # Connecting my_third_task.out_some_other_output to the sub-workflow output
            self.out_my_output.connect(self.my_third_task.out_some_other_output)


Example: Using a Sub-Workflow
-------------------------------

To use a sub-workflow within a workflow, you simply call :meth:`~sciluigi.workflow.WorkflowTask.new_task` as you would
within a workflow, but you pass in the sub-workflow instead of a task.  You can then connect the sub-workflow's inputs
and outputs to the inputs and ouputs of other tasks within your workflow.  The following example illustrates the full
use of a sub-workflow, including the sub-workflow declaration itself as well as the use within a larger workflow.  The
naming follows the naming used in the diagrams presented in the :ref:`overview <sub_workflow_overview>`

Task Code:

.. code-block:: python

    from sciluigi import Task, TargetInfo, TaskInput
    from subprocess import check_call

    class TaskA(Task):

        def initialize_inputs_and_outputs(self):
            # Task A has no inputs
            self.out_task_a_output = TargetInfo(self, 'task_a_output.txt')

        def run(self):
            with self.out_task_a_output.open('w') as f:
                f.write('Writing to output')

    class TaskB(Task):

        def initialize_inputs_and_outputs(self):
            self.in_task_b_input = TaskInput()

            self.out_task_b_output = TargetInfo(self, 'task_b_output.txt')

        def run(self):
            check_call(['cp', self.in_task_b_input.path, self.out_task_b_output.path])

    class TaskC(Task):

        def initialize_inputs_and_outputs(self):
            self.in_task_c_input = TaskInput()

            self.out_task_c_output = TargetInfo(self, 'task_c_output.txt')

        def run(self):
            check_call(['cp', self.in_task_c_input.path, self.out_task_c_output.path])

    class TaskD(Task):

        def initialize_inputs_and_outputs(self):
            # Task D has no outputs
            self.out_task_d_input = TargetInfo(self, 'task_a_output.txt')

        def run(self):
            with self.out_task_d_input.open('r') as f:
                for line in f:
                    print line

Sub-Workflow Code:

.. code-block:: python

    from sciluigi import SubWorkflowOutput, SubWorkflowTask, TaskInput

    from my_tasks import TaskB, TaskC

    class SubWorkflowA(SubWorkflowTask):

        def initialize_tasks(self):
            self.task_b = self.new_task('TaskB', TaskB)
            self.task_c = self.new_task('TaskC', TaskC)

        def initialize_inputs_and_outputs(self):
            self.in_subworkflow_input = TaskInput()

            self.out_subworkflow_output = SubWorkflowOutput(self)

        def connect_tasks(self):
            self.task_b.in_task_b_input.connect(self.in_subworkflow_input)

            self.task_c_.in_task_c_input.connect(self.task_b.out_task_b_output)

            self.out_subworkflow_output.connect(self.task_c.out_task_c_output)

Workflow Code:

.. code-block:: python

    from sciluigi import WorkflowTas

    from my_tasks import TaskA, TaskD
    from my_sub_workflow import SubWorkflowA

    class MyWorkflow(WorkflowTask):

        def workflow(self):
            task_a = self.new_task('TaskA', TaskA)
            subworkflow_a = self.new_task('SubWorkflowA', SubWorkflowA)
            task_d = self.new_task('TaskD', TaskD)

            subworkflow_a.in_subworkflow_input.connect(task_a.out_task_a_output)

            task_d.in_task_d_input.connect(subworkflow_a.out_subworkflow_output)

            return task_d