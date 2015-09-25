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
from .config import *
from .gerda_data import *
from .tier_task import *


class Tier0Output(namedtuple('Tier0Output', ['data', 'checksum', 'runlog'])):
    __slots__ = ()



class Tier0AvailKey(TierKeyTask, luigi.task.ExternalTask):
    def output(self):
        raw_file = self.gerda_data.data_file(self.file_key, 'all', 'tier0')

        return Tier0Output(
            data = luigi.LocalTarget('{file}'.format(file = raw_file)),
            runlog = luigi.LocalTarget('{file}.runlog'.format(file = raw_file)),
            checksum = luigi.LocalTarget('{file}.md5'.format(file = raw_file))
        )
