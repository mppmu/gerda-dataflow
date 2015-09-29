# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Oliver Schulz <oschulz@mpp.mpg.de>
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
#

import os
import subprocess32 as subprocess

from .logger import *


class LocalSubprocess(object):
    def __init__(self, label, program, arguments = [], stdin = None, stdout = None, stderr = None, close_fds = True, pass_fds=()):
        args = [program] + arguments
        logger.debug('Dispatching system task "{label}" in sub-process: {args}'.format(label = label, args = args))
        self.label = label
        self.process = subprocess.Popen(args = args, stdin = stdin, stdout = stdout, stderr = stderr, close_fds = close_fds, pass_fds = pass_fds)


    @property
    def program(self):
        return self.process.args[0]

    @property
    def arguments(self):
        return self.process.args[1:]

    @property
    def stdin(self):
        return self.process.stdin

    @property
    def stdout(self):
        return self.process.stdout

    @property
    def stderr(self):
        return self.process.stderr

    @property
    def returncode(self):
        return self.process.returncode


    def wait(self):
        return self.process.wait()


    def wait_and_check(self, raise_exception = True):
        success = (self.wait() == 0)

        if (success):
            logger.debug('System task "{label}" (executable "{prog}") finished successfully'.format(
                label = self.label, prog = self.program))
            return True
        else:
            msg = 'System task "{label}" (executable "{prog}") failed (exit status {rc})'.format(
                label = self.label, prog = self.program, rc = str(self.process.returncode))
            if raise_exception:
                raise RuntimeError(msg)
            else:
                logger.error(msg)
                return False

