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
from .local_subprocess import *
from .tier_task import *
from .tier1 import *
from .props import *


class Tier2GenSystem(TierSystemTask):
    def __init__(self, *args, **kwargs):
        super(Tier2GenSystem, self).__init__(*args, **kwargs)


    def requires(self):
        return Tier1Gen(self.config, self.file_key)


    def run(self):
        logger.debug('Running Tier2GenSystem for "{key}", system "{system}"'.format(
            key = self.file_key, system = self.system))

        tier2_calib = self.gerda_data.calib_props_for(self.key, 'ged', 'tier2', allow_empty = True)
        gelatio_config = self.gerda_config['proc']['tier2'][self.system]['gelatio']
        Props.add_to(gelatio_config, tier2_calib.get('gelatio', {}))

        ini_file_name = ensure_str(gelatio_config['ini'])
        gelatio_env = gelatio_config.get('env', {})
        logger.debug('Environment variables for execModuleIni: {env}'.format(env = gelatio_env))

        log_target = luigi.LocalTarget(self.gerda_data.log_file(self.key, self.system, 'tier2'))

        log_file = None

        try:
            input_file = self.input().tier1[self.system].open('r')
            output_file = self.output().open('w')
            log_file = log_target.open('w')

            LocalSubprocess(
                label = '{key}_{system}_execModuleIni'.format(key = self.key.name, system = self.system),
                program = 'execModuleIni',
                arguments = ['-o', output_file.name, '-l', log_file.name, ini_file_name, input_file.name],
                env = env_list(additional = gelatio_env)
            ).wait_and_check()

            output_file.close()

        finally:
            if log_file and not log_file.closed: log_file.close()


    def output(self):
        return luigi.LocalTarget(self.gerda_data.data_file(self.key, self.system, 'tier2'))



class Tier2Gen(TierKeyTask, luigi.task.WrapperTask):
    def __init__(self, *args, **kwargs):
        super(Tier2Gen, self).__init__(*args, **kwargs)


    def requires(self):
        systems = self.gerda_config['proc']['tier2'].keys()
        return { system: Tier2GenSystem(self.config, self.file_key, system) for system in systems }
