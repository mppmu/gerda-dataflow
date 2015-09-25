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

from .file_key import *


class Dataset(namedtuple('Dataset', [
    'file_keys',
])):
    __slots__ = ()

    ignore_line_expr = re.compile('^(\s*#.*)?$')

    @staticmethod
    def get(value):
        if isinstance(value, Dataset):
            return value
        elif isinstance(value, list):
            return Dataset([FileKey.get(elem) for elem in value])
        if isinstance(value, basestring):
            return Dataset.read_from(value)
        else:
            raise ValueError("Can't get Dataset from value of type {t}".format(t = type(source)))

    @staticmethod
    def read_from(file_name):
        with open(file_name, 'r') as input:
            keys = [FileKey.get(key_str.strip()) for key_str in input if not Dataset.ignore_line_expr.match(key_str)]
            return Dataset(keys)
