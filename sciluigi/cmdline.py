from luigi.cmdline import luigi_run
from sciluigi.interface import setup_logging
import sys


def sciluigi_run(argv=sys.argv[1:]):
    setup_logging()
    luigi_run(argv)