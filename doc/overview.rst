Getting Started
================

Installing and Upgrading
---------------------------

If you have already installed the `pipelines repo <http://pipelines-docs.h3b.hope>`_, then SciLuigi will already be
installed on your machine.  If you do not wish to install pipelines and want to install SciLuigi independenty, you can
do so via our private `Artifactory <https://www.jfrog.com/artifactory/>`_ instance.  First, you'll need to
`configure pip <http://pipelines-docs.h3b.hope/user_guide.html#installation>`_ to point to our Artifactory instance.

You can then run the following command to install SciLuigi:

.. code-block:: none

    pip install h3sciluigi

**Note that you need to install** ``h3sciluigi`` **instead of** ``sciluigi``.  ``sciluigi`` **is the original package,**
**while** ``h3sciluigi`` **contains our customizations.**

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
