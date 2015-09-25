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
from .tier0 import *


class Tier1Output(namedtuple('Tier1Output', ['data'])):
    __slots__ = ()



class Tier1GenSystem(TierSystemTask):
    def __init__(self, *args, **kwargs):
        super(Tier1GenSystem, self).__init__(*args, **kwargs)


    def requires(self):
        return Tier0AvailKey(self.config, self.file_key)


    def run(self):
        logger.debug('Running Tier1GenSystem for "{key}", system "{system}"'.format(
            key = self.file_key, system = self.system))

        raw2mgdo_config = self.gerda_config['proc'][self.system]['raw2mgdo']
        conversion = ensure_str(raw2mgdo_config['conversion'])
        inverted = ensure_bool(raw2mgdo_config['inverted'])

        with self.input().data.open('r') as input_file:
            with self.output().data.open('w') as output_file:
                raw2mgdo_opts = ['-c', conversion, '-m', '50']
                if inverted: raw2mgdo_opts = raw2mgdo_opts + ['--inverted']
                raw2mgdo_opts = raw2mgdo_opts + [input_file.name, '-f', output_file.name]
                job = run_subprocess('Raw2MGDO', raw2mgdo_opts)


    def output(self):
        return Tier1Output(
            data = luigi.LocalTarget(self.gerda_data.data_file(self.file_key, self.system, 'tier1'))
        )



class Tier1GenKey(TierKeyTask, luigi.task.WrapperTask):
    def __init__(self, *args, **kwargs):
        super(Tier1GenKey, self).__init__(*args, **kwargs)


    def requires(self):
        systems = self.gerda_config['proc'].keys()
        return [ Tier1GenSystem(self.config, self.file_key, system) for system in systems ]
