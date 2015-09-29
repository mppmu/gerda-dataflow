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

from collections import namedtuple

import luigi

from .logger import *
from .process_dispatcher import *
from .tier_task import *
from .tier1 import *


class Tier2Output(namedtuple('Tier2Output', ['tier2'])):
    __slots__ = ()



class Tier2GenSystem(TierSystemTask):
    def __init__(self, *args, **kwargs):
        super(Tier2GenSystem, self).__init__(*args, **kwargs)


    def requires(self):
        return Tier1Gen(self.config, self.file_key)


    def run(self):
        logger.debug('Running Tier2GenSystem for "{key}", system "{system}"'.format(
            key = self.file_key, system = self.system))

        gelatio_config = self.gerda_config['proc'][self.system]['gelatio']
        ini_file_name = ensure_str(gelatio_config['ini'])

        log_target = luigi.LocalTarget(self.gerda_data.log_file(self.file_key, self.system, 'tier2'))

        try:
            input_file = self.input()[self.system].tier1.open('r')
            output_file = self.output().tier2.open('w')
            log_file = log_target.open('w')

            job = run_subprocess('execModuleIni',
                ['-o', output_file.name, '-l', log_file.name, ini_file_name, input_file.name])

            output_file.close()

        finally:
            log_file.close()


    def output(self):
        return Tier2Output(
            tier2 = luigi.LocalTarget(self.gerda_data.data_file(self.file_key, self.system, 'tier2'))
        )



class Tier2Gen(TierKeyTask, luigi.task.WrapperTask):
    def __init__(self, *args, **kwargs):
        super(Tier2Gen, self).__init__(*args, **kwargs)


    def requires(self):
        systems = self.gerda_config['proc'].keys()
        return { system: Tier2GenSystem(self.config, self.file_key, system) for system in systems }
