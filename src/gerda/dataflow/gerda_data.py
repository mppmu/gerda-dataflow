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
import re
import os
import glob

from .props import *
from .config import *
from .file_key import *
from .calib_catalog import *


class GerdaData(namedtuple('GerdaData', ['dataflow_config'])):
    __slots__ = ()

    config_file_name_glob = '*-config.json'
    config_file_name_expr = re.compile('^(.*)-config.json$')


    raw_file_suffix = {
        'phy': '',
        'cal': '-calib',
        'pca': '-pcalib'
    }


    calib_categories = [ 'cal', 'pca' ]


    def raw_data_location(self, setup):
        return self.dataflow_config.setups[setup].data.raw

    def gen_data_location(self, setup):
        return self.dataflow_config.setups[setup].data.gen

    def meta_data_location(self, setup):
        return self.dataflow_config.setups[setup].data.meta


    def data_path_name(self, file_key, system, tier):
        key = FileKey.get(file_key)

        if (tier == 'tier0'):
            return key.run_str
        else:
            return os.path.join(tier, system, key.category, key.run_str)


    def data_base_name(self, file_key, system, tier):
        key = FileKey.get(file_key)

        if (tier == 'tier0'):
            return '{time}.events{suffix}'.format(
                time = arrow.get(key.time).to('UTC').format('YYYYMMDD-HHmmss'),
                suffix = GerdaData.raw_file_suffix[key.category]
            )

        else:
            return '{key}-{system}-{tier}'.format(
                key = key,
                system = system,
                tier = tier
            )


    def data_file_base(self, file_key, system, tier):
        key = FileKey.get(file_key)
        data_location = \
            self.raw_data_location(key.setup) if tier == 'tier0' \
            else self.gen_data_location(key.setup)

        return os.path.join(
            data_location,
            self.data_path_name(key, system, tier),
            self.data_base_name(key, system, tier)
        )


    def data_file(self, file_key, system, tier):
        if (tier == 'tier0'):
            return self.data_file_base(file_key, system, tier) + '.bz2'
        else:
            return self.data_file_base(file_key, system, tier) + '.root'


    def log_file(self, file_key, system, tier, tag = ""):
        if (tier == 'tier0'):
            raise ValueError("Can't provide log file names for tier0")
        else:
            return self.data_file_base(file_key, system, tier) + '.log'


    def calib_file(self, file_key, system, tier):
        key = FileKey.get(file_key)

        if key.category not in GerdaData.calib_categories:
            raise ValueError("Can't generate calib file path for file key of category \"{category}\", should be \"cal\"".format(category = key.category))

        return os.path.join(
            self.meta_data_location(key.setup),
            'calib',
            key.run_str,
            '{key}-{system}-{tier}-calib.json'.format(key = key.name, system = system, tier = tier)
        )


    def calib_catalog_file(self, setup):
        return os.path.join(
            self.meta_data_location(setup),
            'calib',
            '{}-calibrations.jsonl'.format(setup)
        )


    def calib_catalog(self, setup):
        return CalibCatalog.read_from(self.calib_catalog_file(setup))


    def calib_file_for(self, file_key, system, tier, allow_none = False):
        key = FileKey.get(file_key)
        calib_catalog = self.calib_catalog(key.setup)
        calib_key = calib_catalog.calib_for(key, system, allow_none)
        if allow_none and calib_key is None: return None
        else: return self.calib_file(calib_key, system, tier)


    def calib_props_for(self, file_key, system, tier, allow_empty = False):
        calib_file = self.calib_file_for(file_key, system, tier, allow_empty)
        if calib_file is None:
            assert(allow_empty)
            return {}
        else:
            if os.path.isfile(calib_file):
                return Props.read_from([calib_file], subst_pathvar = True, subst_env = True, trim_null = True)
            else:
                if allow_empty: return {}
                else: raise RuntimeError("No valid calibration found for file key \"{key}\", system \"{system}\", tier \"{tier}\"".format(key = key.name, system = system, tier = tier))


    def config_files(self, file_key):
        key = FileKey.get(file_key)

        cfg_files = glob.iglob(os.path.join(self.meta_data_location(key.setup), 'config', GerdaData.config_file_name_glob))

        def matches_key(file_name):
            data_base_name = os.path.basename(file_name)
            m_cfg_key_part = GerdaData.config_file_name_expr.match(data_base_name)
            if m_cfg_key_part != None:
                grp = m_cfg_key_part.group(1)
                return key.matches(grp)
            else:
                return False

        return sorted([ f for f in cfg_files if matches_key(f) ])


    def config(self, file_key):
        key = FileKey.get(file_key)
        return Props.read_from(self.config_files(key), subst_pathvar = True, subst_env = True, trim_null = True)
