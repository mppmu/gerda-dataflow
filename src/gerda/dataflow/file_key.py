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

from .util import *


class FileKey(namedtuple('FileKey', ['setup', 'run', 'time', 'category'])):
    __slots__ = ()

    wildcard = '%'
    file_key_expr = re.compile('^([A-Za-z_]+)-run([0-9]{4})-([0-9]{8}T[0-9]{6}Z)-([A-Za-z_]+)$')
    file_key_wildcard_expr = re.compile('^(%|[A-Za-z_]+)-(%|run[0-9]{4})-(%|[0-9]{8}T[0-9]{6}Z)-(%|[A-Za-z_]+)$')


    @staticmethod
    def get(value):
        if isinstance(value, FileKey):
            return value

        elif isinstance(value, basestring):
            m = FileKey.file_key_expr.match(value)

            if m is None:
                raise ValueError("Invalid string value for FileKey: {value}".format(value=value))

            setup = m.group(1)
            run = int(m.group(2))
            time = unix_time(m.group(3))
            category = ensure_str(m.group(4))

            return FileKey(setup, run, time, category)

        else:
            raise ValueError("Can't create FileKey from value of type {t}".format(t = type(value)))


    @property
    def name(self):
        return '{setup}-run{run}-{time}-{category}'.format(
            setup = self.setup,
            run = self.run_str,
            time = self.time_str,
            category = self.category
        )


    @property
    def run_str(self):
        return '{r:04d}'.format(r = self.run)

    @property
    def time_str(self):
        return utc_time_str(self.time)

    @property
    def parts(self):
        return (self.setup, self.run_str, self.time_str, self.category)


    def matches(self, wildcard_key):
        m = FileKey.file_key_wildcard_expr.match(wildcard_key)

        if m is None:
            return False
        else:
            that_parts = m.groups()
            this_parts = self.parts
            if len(that_parts) > len(this_parts):
                return false
            else:
                return all([
                    elem[0] == FileKey.wildcard or elem[0] == elem[1]
                    for elem in zip(that_parts, this_parts)
                ])

    def __str__(self):
        return self.name
