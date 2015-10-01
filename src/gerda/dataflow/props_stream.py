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

import types
import collections
import json

from .logger import *


class PropsStream():
    @staticmethod
    def get(value):
        if isinstance(value, basestring):
            return PropsStream.read_from(value)
        elif isinstance(value, collections.Sequence) or isinstance(value, types.GeneratorType):
            return value
        else:
            raise ValueError("Can't get PropsStream from value of type {t}".format(t = type(source)))


    @staticmethod
    def read_from(file_name):
        logger.debug("Reading JSON Lines file \"{file_name}\".".format(file_name = file_name))
        with open(file_name, 'r') as file:
            for json_str in file:
                yield json.loads(json_str)
