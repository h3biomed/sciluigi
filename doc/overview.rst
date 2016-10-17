Getting Started
================

Installation and Upgrading
---------------------------

Currently, the best way to install ``sciluigi`` and all of its dependencies is through the pre-built wheels stored
on the shared drive.  The command is currently a bit long, but we're hoping to make this process easier in the future
by abstracting all our packaging logic through a service such as Artifactory.

For now, use the following command to install ``sciluigi``:

.. code-block:: none

    pip install --upgrade -I --no-index --find-links /h3/g/bi/projects/pipelines/wheels sciluigi

Overview
---------

The SciLuigi library is really a lightweight wrapper around the Luigi framework.  The key difference from standard
Luigi is that SciLuigi tasks are defined independently of workflows.  In other words, users can define tasks to require
a certain type of input, such as a FASTQ file, rather than hard-coding them to depend on a specific task.  Then, in a
separate workflow definition, users can stitch tasks together.  This pattern is much more flexible and works better
certain H3 pipelines, such as Xenotools.

In addition, this library allows for the creation of "sub-workflows" within a larger workflow.  This pattern comes in
handy if a certain set of tasks are always run in the same order and used within several different workflows.

Running a Workflow
-------------------

SciLuigi can be run from the command line just like Luigi
(see `Luigi guide <http://luigi.readthedocs.io/en/stable/command_line.html>`_).  Just replace the ``luigi`` command
with ``sciluigi``.  In addition, note that ``MyTask`` should be a workflow when running SciLuigi, not a task.  Se the
remainder of this documentation for more info on the differences between tasks and workflows.
