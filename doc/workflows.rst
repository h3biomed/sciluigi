Workflows
==========

.. _workflow_overview:

Overview
---------

A workflow stitches all of your :doc:`tasks` together and can be run using the ``sciluigi`` command.    In addition,
workflows can have inputs and outputs, just like a task.  This feature allows workflows to be nested inside larger
workflows, allowing you to easily "package" several tasks together in a reusable mini-workflow that can then be
injected into any larger workflow.

The image below illustrates the concept of nested workflows by showing a child workflow from the perspective of a
parent workflow.  The parent workflow in this example consists of three components: Task A, Sub-Workflow A (which is a
child workflow), and Task D.  The parent workflow connects the inputs and outputs of these components together in such
a way that data flows out of Task A into Sub-Workflow A, and then out of Sub-Workflow A into Task D.  Although
Sub-Workflow A is really its own workflow, you can see from the diagram that it can be treated just like any other task
within the parent workflow.

.. figure:: SubWorkflow.png

However, the child workflow really consists of multiple tasks, as shown in the next image.  The child workflow is a
mini-workflow where Task B passes its output into the input of Task C.  It gets a little tricky because there is the
need to differentiate between workflow inputs/outputs and task inputs/outputs.  The workflow inputs and outputs
are indicated with the purple arrows, while the task inputs and outputs are indicated with the dark blue arrows.  Task
B requires an input, and it gets that input from the input of the workflow as a whole.  Similarly, Task C produces
an output, which it passes on to the output of the workflow as a whole.  It is this process of connecting the
inputs and outputs of the tasks within a child workflow to the inputs and outputs of the child workflow itself that
allows the parent workflow to treat the child workflow as an individual task.

.. figure:: FullSubWorkflow.png

Declaring a Workflow
---------------------

The first step is to declare your workflow class.  A workflow should inherit from the
:class:`~sciluigi.workflow.WorkflowTask` class.  Like a regular :doc:`SciLuigi task <tasks>`, a workflow can accept
Luigi parameters.  If you run your workflow from the command line, these parameters can be passed in as command-line
arguments, as in the following example:

Code:

.. code-block:: python

    import luigi
    from sciluigi import WorkflowTask

    class MyWorkflow(WorkflowTask):

        my_param = luigi.Parameter()
        my_other_param = luigi.Parameter()

        # Rest of workflow code goes here

Command line:

.. code-block:: none

    sciluigi --module mypackage.mymodule MyWorkflow --my-param foo --my-other-param bar

Initializing Tasks
------------------

The first step is to initialize all of the tasks that will make up your workflow.  To do this, you'll need to
implement the :meth:`~sciluigi.workflow.WorkflowTask.initialize_tasks` method.  Within this method, you
declare tasks by calling the :func:`~sciluigi.task.new_task` function, as in the following example:

.. code-block:: python

    wf_props = {'wf_name': self.instance_name, 'wf_foo': self.foo}
    self.my_first_task = new_task('My Task Name', MyTask, task_param='foo', wf_props, another_task_param='bar')
    self.my_second_task = new_task('My Other Task Name', MyOtherTask, wf_props, yet_another_task_param='baz')
    self.my_third_task = new_task('My Third Task Name', MyThirdTask, wf_props)

The arguments to the :func:`~sciluigi.task.new_task` function consist of:

- **The task name.**  This can really be anything you want, but you should make it unique.  This name will be one of the
  criteria used by the Luigi scheduler to determine the uniqueness of a task.  Therefore, if you initialize two
  identical tasks with identical param values but different names, Luigi will interpret them as two completely
  different tasks.

- **The task itself.**  This is the task class that will be run.

- **Properties of this workflow**  This can be an object of any type (i.e. a dict) that contains workflow-level
  properties that the new task will need to reference.  The advantage of using a properties object rather than normal
  Luigi parameters (see the next bullet point) is that you can pass the same workflow-level properties to all of the
  workflow's tasks without redundantly defining corresponding parameters on each task and passing them in.

- **The task parameters, if necessary.**  If your task has any parameters declared with ``luigi.Parameter()``, the
  value of those parameters would be passed in as named arguments here.

Declaring Inputs and Outputs
-----------------------------

Once your tasks have been initialized, you need to declare your inputs and outputs.  You must do this within an
:meth:`~sciluigi.task.Task.initialize_inputs_and_outputs` method.  The format for this declaration is
:ref:`nearly the same as for the input/output declaration of a task <task_inputs_outputs>`.  The big difference is
that, instead of assigning a :class:`~sciluigi.dependencies.TargetInfo` or an
:class:`~sciluigi.dependencies.S3TargetInfo` to the outputs, you assign a
:class:`~sciluigi.dependencies.WorkflowOutput`.  You must do this because the outputs of a workflow will
actually be outputs of one or more tasks within the workflow (see the example in the
:ref:`overview <workflow_overview>` where the workflow output was actually connected to the output of Task C).
**As with tasks, all input names must begin with** ``in_`` **and all output names must begin with** ``out_``.
**In addition, a workflow MUST declare at least one output.**  A task will not run simply because it was declared in
a workflow.  Instead, a workflow's outputs determine which tasks run.  **If you leave a task "hanging" so that it does**
**not connect to any downstream tasks or to any workflow outputs, it will NOT run.**

.. code-block:: python

    from sciluigi import TaskInput
    from sciluigi import WorkflowOutput

    def initialize_inputs_and_outputs(self):
        self.in_my_input = TaskInput()

        self.out_my_output = WorkflowOutput(self)

Connecting Tasks, Inputs, and Outputs
--------------------------------------

Finally, you must connect your tasks, inputs, and outputs together.  This must be done in a separate
:meth:`~sciluigi.workflow.WorkflowTask.connect_tasks` method, as in the following example:

.. code-block:: python

    def connect_tasks(self):
        self.my_first_task.in_some_input.receive_from(self.in_my_input)
        self.my_second_task.in_some_other_input.receive_from(my_first_task.out_some_output)

        self.my_third_task.in_input_1.receive_from(my_second_task.out_output_1)
        self.my_third_task.in_input_2.receive_from(my_second_task.out_output2)

        self.out_my_output.receive_from(my_third_task.out_some_other_output)

The syntax is ``<DOWNSTREAM TASK>.<INPUT NAME>.receive_from(<UPSTREAM TASK>.<OUTPUT NAME>)``.  This syntax uses the
``receive_from`` method of :class:`~sciluigi.dependencies.TaskInput`.  (Recall from :doc:`tasks` that all task inputs
should have a :class:`~sciluigi.dependencies.TaskInput` assigned to them.)  When you call this method, it lets SciLuigi know
that the given input port of the downstream task should be connected to the output port of the upstream task.  When the
upstream task spits out an output, that output will then be fed into the downstream task.  Alternatively, you can use
the syntax ``<UPSTREAM TASK>.<OUTPUT NAME>.send_to(<DOWNSTREAM TASK>.<INPUT NAME>)`` if you find it more intuitive.

While most inputs will only be connected to one output, it is possible to define inputs in a task that can be connected
to multiple files.  To connect multiple files, simply call the :meth:`~sciluigi.dependencies.TaskInput.receive_from`
method or the :meth:`~sciluigi.dependencies.TargetInfo.send_to` method multiple times, as in the following example:

.. code-block:: python

    self.a_task.in_an_input.receive_from(another_task.out_an_output)
    self.a_task.in_an_input.receive_from(yet_another_task.out_yet_another_output)


Example: Nesting Workflows
-------------------------------

To use a child workflow within a parent workflow, you simply call :func:`~sciluigi.workflow.task.new_task`, but you
pass in the child workflow instead of a task.  You can then connect the child workflow's inputs
and outputs to the inputs and ouputs of other tasks within your workflow.  The following example illustrates the full
use of nested workflows, including the child workflow declaration itself as well as the use within a larger workflow.
The naming follows the naming used in the diagrams presented in the :ref:`overview <workflow_overview>`

**Task Code:**

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
            self.out_task_d_input = TaskInput()

            self.out_task_d_output = TargetInfo(self, 'task_d_output.txt')

        def run(self):
            with self.out_task_d_input.open('r') as f:
                with self.out_task_d_output.open('w') as outfile:
                    for line in f:
                        outfile.write(line)

**Child Workflow Code:**

.. code-block:: python

    from sciluigi import new_task, WorkflowOutput, WorkflowTask, TaskInput

    from my_tasks import TaskB, TaskC

    class ChildWorkflowA(WorkflowTask):

        def initialize_tasks(self):
            self.task_b = new_task('TaskB', TaskB, self.workflow_properties)
            self.task_c = new_task('TaskC', TaskC, self.workflow_properties)

        def initialize_inputs_and_outputs(self):
            self.in_child_workflow_input = TaskInput()

            self.out_child_workflow_output = WorkflowOutput(self)

        def connect_tasks(self):
            self.task_b.in_task_b_input.receive_from(self.in_child_workflow_input)

            self.task_c_.in_task_c_input.receive_from(self.task_b.out_task_b_output)

            self.out_child_workflow_output.receive_from(self.task_c.out_task_c_output)

**Workflow Code:**

.. code-block:: python

    from sciluigi import new_task, WorkflowTask, WorkflowOutput

    from my_tasks import TaskA, TaskD
    from my_child_workflow import ChildWorkflowA

    class MyWorkflow(WorkflowTask):

        def initialize_tasks(self):
            wf_props = {'wf_name': 'foo', 'wf_bar': 'baz'}
            self.task_a = new_task('TaskA', TaskA, wf_props)
            self.child_workflow_a = new_task('ChildWorkflowA', ChildWorkflowA, wf_props)
            self.task_d = new_task('TaskD', TaskD, wf_props)

        def initialize_inputs_and_outputs(self):
            self.out_output_file = WorkflowOutput(self)

        def connect_tasks(self):
            self.child_workflow_a.in_child_workflow_input.receive_from(self.task_a.out_task_a_output)

            self.task_d.in_task_d_input.receive_from(self.child_workflow_a.out_child_workflow_output)

            self.out_output_file.receive_from(self.task_d.out_task_d_output)
