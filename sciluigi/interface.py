'''
This module contains mappings of methods that are part of the sciluigi API
'''

import luigi
import logging
import sciluigi.util

LOGFMT_LUIGI = '%(asctime)s | %(levelname)8s |    LUIGI | %(message)s'
LOGFMT_SCILUIGI = '%(asctime)s | %(levelname)8s | SCILUIGI | %(message)s'
DATEFMT = '%Y-%m-%d %H:%M:%S'


def setup_logging():
    # Some code adapted from Luigi
    #
    # Copyright 2012-2015 Spotify AB
    #
    # Licensed under the Apache License, Version 2.0 (the "License");
    # you may not use this file except in compliance with the License.
    # You may obtain a copy of the License at
    #
    # http://www.apache.org/licenses/LICENSE-2.0
    #
    # Unless required by applicable law or agreed to in writing, software
    # distributed under the License is distributed on an "AS IS" BASIS,
    # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    # See the License for the specific language governing permissions and
    # limitations under the License.

    # use a variable in the function object to determine if it has run before
    if getattr(setup_logging, 'has_run', False):
        return

    # Formatter
    luigi_log_formatter = logging.Formatter(LOGFMT_LUIGI, DATEFMT)
    sciluigi_log_formatter = logging.Formatter(LOGFMT_SCILUIGI, DATEFMT)

    # Stream handler (for STDERR)
    luigi_stream_handler = logging.StreamHandler()
    luigi_stream_handler.setFormatter(luigi_log_formatter)
    luigi_stream_handler.setLevel(logging.INFO)

    sciluigi_stream_handler = logging.StreamHandler()
    sciluigi_stream_handler.setFormatter(sciluigi_log_formatter)
    sciluigi_stream_handler.setLevel(logging.DEBUG)

    # Loggers
    luigi_logger = logging.getLogger('luigi-interface')
    luigi_logger.addHandler(luigi_stream_handler)
    luigi_logger.setLevel(logging.DEBUG)
    luigi.interface.setup_interface_logging.has_run = True

    sciluigi_logger = logging.getLogger('sciluigi-interface')
    sciluigi_logger.addHandler(sciluigi_stream_handler)
    sciluigi_logger.setLevel(logging.DEBUG)

    setattr(setup_logging, 'has_run', True)

setup_logging()


def run(*args, **kwargs):
    '''
    Forwarding luigi's run method
    '''
    luigi.run(*args, **kwargs)


def run_local(*args, **kwargs):
    '''
    Forwarding luigi's run method, with local scheduler
    '''
    run(local_scheduler=True, *args, **kwargs)
