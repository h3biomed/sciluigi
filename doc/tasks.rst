Tasks, Inputs, and Outputs
===========================

Overview of SciLuigi Tasks
---------------------------

A task is the basic unit of execution in your pipeline.  A task will generally have both inputs and outputs, but isn't
required to have either.  An example of a task could be a STAR task that takes one or more FASTQ files as input, runs
the STAR alignment tool, and produces several output files, including the aligned BAM file.

SciLuigi tasks are different from the Luigi tasks you may be familiar with from the previous generation of H3 pipelines.
Luigi tasks depend on other tasks, but SciLuigi tasks depend on specific inputs.  For example, the STAR task mentioned
above might depend on one or more DownloadFastQ tasks in the Luigi framework.  However, in the SciLuigi framework, the
STAR task would depend on one or more FASTQ inputs.  In other words, the SciLuigi version of the STAR task doesn't care
what specific task provides input.  All it needs are FASTQ files it can feed into the STAR tool.

Working with Task Inputs and Outputs
-------------------------------------

The biggest structural difference between SciLuigi tasks and Luigi tasks is that SciLuigi tasks do not declare
``requires`` and ``ouptut``.  Instead, a simple ``initialize_inputs_and_outputs`` method is called.  In this method,
all inputs and outputs for this task should be declared as attributes attached to ``self``, as in the following example:

.. code-block:: python

    from sciluigi import TaskInput
    from sciluigi import TargetInfo

    def initialize_inputs_and_outputs(self):
        self.in_fq_file = TaskInput()
        self.in_gtf_file = TaskInput()

        self.out_bam_file = TargetInfo(self, 'output.bam')

Note that the input variable names begin with ``in_`` and the output variable name begins with ``out_``.  **All input
and output variables MUST follow this naming convention, as that is how SciLuigi parses inputs and outputs.**

In addition, take note of the objects that are assigned to these inputs and outputs.  A
:class:`~sciluigi.dependencies.TaskInput` object should be attached to all inputs.  Meanwhile either a
:class:`~sciluigi.dependencies.TargetInfo` or an :class:`~sciluigi.dependencies.S3TargetInfo` object should be attached
to all outputs.  The object you choose depends on whether the output is a local file or a file stored in S3.

The :class:`~sciluigi.dependencies.TaskInput` object will be discussed in the :doc:`workflows documentation <workflows>`.

The :class:`~sciluigi.dependencies.TargetInfo` or an :class:`~sciluigi.dependencies.S3TargetInfo` objects are thin
wrappers around Luigi's ``Target``
