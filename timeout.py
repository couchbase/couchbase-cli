#
# Copyright 2012, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import signal
import os
from threading import Timer


class TimeoutException(Exception):

    """Raise exception if execution time of function or method exceeds
    specified timeout.
    """

    def __init__(self, command):
        self.command = command

    def __str__(self):
        if self.command == 'bucket-list':
            return 'bucket is not reachable or does not exist'
        elif self.command == 'bucket-create':
            return 'most likely bucket is not created'
        elif self.command == 'bucket-edit':
            return 'most likely bucket parameters are not changed'
        elif self.command == 'bucket-delete':
            return 'most likely bucket is not deleted'
        elif self.command == 'bucket-flush':
            return 'most likely bucket is not flushed'
        else:
            return 'unknown error'


def timed_out(timeout=60):
    def decorator(function):
        def wrapper(*args, **kargs):
            """Start timer thread which sends SIGABRT signal to current
            process after defined timeout. Create signal listener which raises
            exception once signal is received. Run actual task in parallel.
            """

            def send_signal():
                pid = os.getpid()
                os.kill(pid, signal.SIGABRT)

            def handle_signal(signum, frame):
                raise TimeoutException(args[1])

            timer = Timer(timeout, send_signal)
            timer.start()
            signal.signal(signal.SIGABRT, handle_signal)

            try:
                return function(*args, **kargs)
            finally:
                timer.cancel()
        return wrapper
    return decorator
