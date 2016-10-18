Sub-Workflows
==============

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
