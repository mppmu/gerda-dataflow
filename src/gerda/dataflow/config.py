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

from .props import *
from .util import *


class DataflowConfig(namedtuple('DataflowConfig', [
    'setups',
])):
    __slots__ = ()

    @staticmethod
    def get(source = None):
        if isinstance(source, DataflowConfig):
            return source
        if isinstance(source, basestring):
            if source:
                props = Props.read_from(source, subst_pathvar = True, subst_env = True)
                return DataflowConfig.get(props)
            else:
                return DataflowConfig.get(None)
        elif isinstance(source, dict):
            return DataflowConfig(
                setups = { ensure_str(k): SetupConfig.get(v) for k, v in source['setups'].iteritems() }
            )
        elif source is None:
            cfg_file_name = os.getenv('DATAFLOW_CONFIG')
            if cfg_file_name is None:
                raise RuntimeError("Environment variable \"DATAFLOW_CONFIG\" not set.")
            return DataflowConfig.get(cfg_file_name)
        else:
            raise ValueError("Can't get DataflowConfig from value of type {t}".format(t = type(source)))


class SetupConfig(namedtuple('SetupConfig', [
    'data'
])):
    __slots__ = ()

    @staticmethod
    def get(source):
        if isinstance(source, SetupConfig):
            return source
        elif isinstance(source, dict):
            return SetupConfig(
                data = DataLocations.get(source['data'])
            )
        else:
            raise ValueError("Can't get SetupConfig from value of type {t}".format(t = type(source)))



class DataLocations(namedtuple('DataLocations', [
    'raw',
    'gen',
    'meta'
])):
    __slots__ = ()

    @staticmethod
    def get(source):
        if isinstance(source, DataLocations):
            return source
        elif isinstance(source, dict):
            return DataLocations(
                raw = ensure_str(source['raw']),
                gen = ensure_str(source['gen']),
                meta = ensure_str(source['meta'])
            )
        else:
            raise ValueError("Can't get DataLocations from value of type {t}".format(t = type(source)))
