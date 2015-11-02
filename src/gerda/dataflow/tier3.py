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
from .env import *
from .tier_task import *
from .tier2 import *


class Tier3Gen(TierKeyTask):
    def __init__(self, *args, **kwargs):
        super(Tier3Gen, self).__init__(*args, **kwargs)


    def requires(self):
        tier2_systems = self.gerda_config['proc']['tier2'].keys()
        return { system: Tier2GenSystem(self.config, self.file_key, system) for system in tier2_systems }


    def run(self):
        logger.debug('Running Tier3Gen for "{key}"'.format(key = self.file_key))

        tier3_config = self.gerda_config['proc']['tier3']['all']
        cuts = tier3_config['buildTier3']['cuts']
        ged_runcfg_dir = tier3_config['buildTier3']['geruncfg']

        ged_calib_file = self.gerda_data.calib_file_for(self.key, 'ged', 'tier3')
        pmt_calib_file = self.gerda_data.calib_file_for(self.key, 'pmt', 'tier3', allow_none = True)

        log_target = luigi.LocalTarget(self.gerda_data.log_file(self.key, 'all', 'tier3'))

        log_file = None

        try:
            input = self.input()

            ged_only = not ('pmt' in input and 'spm' in input)

            output_file = self.output().open('w')
            log_file = log_target.open('w')

            ged_input_file = input['ged'].open('r')

            arguments = [
                '-c', cuts,
                '-e', ged_calib_file
            ]

            if (pmt_calib_file is not None):
                arguments = arguments + ['-v', pmt_calib_file]

            arguments = arguments + [
                '-o', output_file.name,
                ged_input_file.name
            ]

            if not ged_only:
                pmt_input_file = input['pmt'].open('r')
                spm_input_file = input['spm'].open('r')
                arguments = arguments + [pmt_input_file.name, spm_input_file.name]

            LocalSubprocess(
                label = '{key}_all_buildTier3'.format(key = self.key.name),
                program = 'buildTier3',
                arguments = arguments,
                stdout = log_file, stderr = subprocess.STDOUT,
                env = env_list(additional = {'MU_CAL': ged_runcfg_dir})
            ).wait_and_check()

            output_file.close()

        finally:
            if log_file and not log_file.closed: log_file.close()


    def output(self):
        return luigi.LocalTarget(self.gerda_data.data_file(self.key, 'all', 'tier3'))
