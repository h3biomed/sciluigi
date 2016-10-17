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


