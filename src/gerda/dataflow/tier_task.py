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

from .dataflow_task import *
from .gerda_data import *


class TierKeyTask(DataflowTask):
    file_key = luigi.Parameter(description="file key to be processed.")

    def __init__(self, *args, **kwargs):
        super(TierKeyTask, self).__init__(*args, **kwargs)

        self.key = FileKey.get(self.file_key)
        self.gerda_data = GerdaData(self.dataflow_config)
        self.gerda_config = self.gerda_data.config(self.key)



class TierSystemTask(TierKeyTask):
    system = luigi.Parameter(description="name of setup system.")

    def __init__(self, *args, **kwargs):
        super(TierSystemTask, self).__init__(*args, **kwargs)
