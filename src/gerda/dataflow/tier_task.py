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

import luigi

from .config import *
from .gerda_data import *


class TierTask(luigi.Task):
    config = luigi.Parameter(default=None)
    file_key = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super(TierTask, self).__init__(*args, **kwargs)

        self.dataflow_config = DataflowConfig.get(self.config)
        self.key = FileKey.get(self.file_key)
        self.gerda_data = GerdaData(self.dataflow_config)
        self.gerda_config = self.gerda_data.config(self.key)



class TierSystemTask(TierTask):
    system = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super(TierSystemTask, self).__init__(*args, **kwargs)
