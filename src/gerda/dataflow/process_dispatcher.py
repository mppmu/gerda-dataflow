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

import saga
import subprocess32 as subprocess

from .logger import *


def run_subprocess(program, arguments):
    logger.debug('Running in sub-process: {p} {a}'.format(p = program, a = arguments))

    process = subprocess.Popen([program] + arguments)
    process.wait()

    if (process.returncode == 0):
        logger.debug('Execution of "{prog}" finished successfully'.format(prog = process.args[0]))
    else:
        raise RuntimeError('Execution of "{prog}"  failed (exit status {rc})'.format(
            prog = process.args[0], rc = str(process.returncode)))


class SagaDispatcher:
    jobService = None

    def __init__(self, serviceURL = 'fork://localhost'):
        self.jobService = saga.job.Service(serviceURL)

    def run(self, label, program, arguments):
        logger.debug('Dispatching SAGA task {l}: {p} {a}'.format(l = label, p = program, a = arguments))

        jobDesc = saga.job.Description()
        jobDesc.environment = {}
        jobDesc.executable = program
        jobDesc.arguments = arguments
        jobDesc.output = label + '.stdout'
        jobDesc.error = label + '.stderr'
        jobDesc.working_directory = os.getcwd()

        job = self.jobService.create_job(jobDesc)
        job.run()
        return job

    def run_blocking(self, label, program, arguments):
        job = self.run(label, program, arguments)

        job.wait()
        if (job.exit_code != 0):
            raise RuntimeError('Dispatched SAGA task "{label}" (executable "{prog}") terminated with exit status {rc}'.format(
                label = label, prog = job.description.executable, rc = str(job.exit_code)))

        return job


# sagaDispatcher = SagaDispatcher()



class SubprocessDispatcher():
    def run_blocking(self, label, program, arguments):
        logger.debug('Dispatching system task "{l}" in sub-process: {p} {a}'.format(l = label, p = program, a = arguments))
        with open(label + '.stdout', 'w') as outFile:
            with open(label + '.stderr', 'w') as errFile:
                outFile = open(label + '.stdout', 'w')
                errFile = open(label + '.stderr', 'w')
                process = subprocess.Popen([program] + arguments, stdout = outFile, stderr = errFile)

                process.wait()

                if (process.returncode == 0):
                    logger.debug('System task "{label}" (executable "{prog}") finished successfully'.format(
                        label = label, prog = process.args[0]))
                else:
                    raise RuntimeError('System task "{label}" (executable "{prog}") failed (exit status {rc})'.format(
                        label = label, prog = process.args[0], rc = str(process.returncode)))


# subprocessDispatcher = SubprocessDispatcher()
