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
from .tier3 import *


class Tier4Gen(TierKeyTask):
    def __init__(self, *args, **kwargs):
        super(Tier4Gen, self).__init__(*args, **kwargs)


    def requires(self):
        return Tier3Gen(self.config, self.file_key)


    def run(self):
        logger.debug('Running Tier4Gen for "{key}"'.format(key = self.file_key))

        tier3_config = self.gerda_config['proc']['tier3']['all']
        ged_runcfg_dir = tier3_config['buildTier3']['geruncfg']

        tier4_config = self.gerda_config['proc']['tier4']['all']
        buildTier4_cfg = tier4_config['buildTier4']
        psdclassfdata = buildTier4_cfg.get('psdclassfdata')

        ged_psd_file = self.gerda_data.calib_file_for(self.key, 'ged', 'tier4')
        pmt_threshold_file = self.gerda_data.calib_file_for(self.key, 'pmt', 'tier4')

        log_target = luigi.LocalTarget(self.gerda_data.log_file(self.key, 'all', 'tier4'))

        log_file = None

        try:
            input = self.input()

            output_file = self.output().open('w')
            log_file = log_target.open('w')

            input_file = input.open('r')

            arguments = []

            arguments = arguments + [
                '-p', ged_psd_file,
                '-t', pmt_threshold_file
            ]

            arguments = arguments + [
                '-o', output_file.name,
                input_file.name
            ]

            if (psdclassfdata is not None):
                arguments = arguments + ['-m', psdclassfdata]

            LocalSubprocess(
                label = '{key}_all_buildTier4'.format(key = self.key.name),
                program = 'buildTier4',
                arguments = arguments,
                stdout = log_file, stderr = subprocess.STDOUT,
                env = env_list(additional = {'MU_CAL': ged_runcfg_dir})
            ).wait_and_check()

            output_file.close()

        finally:
            if log_file and not log_file.closed: log_file.close()


    def output(self):
        return luigi.LocalTarget(self.gerda_data.data_file(self.key, 'all', 'tier4'))
