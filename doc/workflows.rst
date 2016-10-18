Workflows
==========

Overview
---------

A workflow stitches all of your :doc:`tasks` and :doc:`subworkflows` together.  Unlike tasks and sub-workflows, a
workflow cannot have any inputs or outputs, and therefore a workflow is designed to be run independently, without being
connected to any other tasks.

A workflow must accomplish two things:

- Declare all tasks that will be run in the workflow

- Connect the inputs and outputs of these tasks together to ensure the proper flow of data

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

Composing the Workflow Body
----------------------------

All of a workflow's logic must be placed inside of a :meth:`~sciluigi.workflow.WorkflowTask.workflow` method.  As
stated in the overview, this method has two goals.  The first goal is to declare all tasks, and this goal can be
accomplished by calling the workflow's :meth:`~sciluigi.workflow.WorkflowTask.new_task` method, as in the following
example:

.. code-block:: python

    my_first_task = self.new_task('My Task Name', MyTask, task_param='foo', another_task_param='bar')
    my_second_task = self.new_task('My Other Task Name', MyOtherTask, yet_another_task_param='baz')
    my_third_task = self.new_task('My Third Task Name', MyThirdTask)

The arguments to the :meth:`~sciluigi.workflow.WorkflowTask.new_task` method consist of:

- **The task name.**  This can really be anything you want, but you should make it unique.  This name will be one of the
  criteria used by the Luigi scheduler to determine the uniqueness of a task.  Therefore, if you initialize two
  identical tasks with identical param values but different names, Luigi will interpret them as two completely
  different tasks.

- **The task itself.**  This is the task class that will be run.

- **The task parameters, if necessary.**  If your task has any parameters declared with ``luigi.Parameter()``, the
  value of those parameters would be passed in as named arguments here.

.. _connect_tasks:

Next, you'll need to connect the tasks inputs and outputs together, as in the following example:

.. code-block:: python

    my_second_task.in_some_input.connect(my_first_task.out_some_output)

    my_third_task.in_input_1.connect(my_second_task.out_output_1)
    my_third_task.in_input_2.connect(my_second_task.out_output2)

The syntax here is ``<DOWNSTREAM TASK>.<INPUT NAME>.connect(<UPSTREAM TASK>.<OUTPUT NAME>)``.  This syntax uses the
``connect`` method of :class:`~sciluigi.dependencies.TaskInput`.  (Recall from :doc:`tasks` that all task inputs should
have a :class:`~sciluigi.dependencies.TaskInput` assigned to them.)  When you call this method, it lets SciLuigi know
that the given input port of the downstream task should be connected to the output port of the upstream task.  When the
upstream task spits out an output, that output will then be fed into the downstream task.

While most inputs will only be connected to one output, it is possible to define inputs in a task that can be connected
to multiple files.  To connect multiple files, simply call the :meth:`~sciluigi.dependencies.TaskInput.connect` method
multiple times, as in the following example:

.. code-block:: python

    a_task.in_an_input.connect(another_task.out_an_output)
    a_task.in_an_input.connect(yet_another_task.out_yet_another_output)

Finally, the :meth:`~sciluigi.workflow.WorkflowTask.workflow` method needs to return all tasks that server as endpoints
for your workflow.  An endpoint means a task that does not pass on any output to a downstream task.

Putting it all together, a workflow should look something like this:

.. code-block:: python

    import luigi
    from sciluigi import WorkflowTask

    class MyWorkflow(WorkflowTask):

        my_param = luigi.Parameter()
        my_other_param = luigi.Parameter()

        def workflow(self):

            my_first_task = self.new_task('My Task Name', MyTask, task_param=self.my_param, another_task_param=self.my_other_param)
            my_second_task = self.new_task('My Other Task Name', MyOtherTask, yet_another_task_param='baz')
            my_third_task = self.new_task('My Third Task Name', MyThirdTask)

            my_second_task.in_some_input.connect(my_first_task.out_some_output)

            my_third_task.in_input_1.connect(my_second_task.out_output_1)
            my_third_task.in_input_2.connect(my_second_task.out_output2)

            return my_third_task # You MUST return this task to SciLuigi to tell it that this task is an endpoint
