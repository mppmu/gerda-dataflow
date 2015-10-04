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
import bisect

from .file_key import *
from .props_stream import *
from .util import *


class CalibCatalog(namedtuple('CalibCatalog', ['entries'])):
    __slots__ = ()

    class Entry(namedtuple('Entry', ['key', 'valid_from'])):
        __slots__ = ()


    @staticmethod
    def get(value):
        if isinstance(value, CalibCatalog):
            return value
        if isinstance(value, basestring):
            return CalibCatalog.read_from(value)
        else:
            raise ValueError("Can't get CalibCatalog from value of type {t}".format(t = type(source)))


    @staticmethod
    def read_from(file_name):
        return CalibCatalog(
            entries = sorted(
                [
                    CalibCatalog.Entry(FileKey.get(props['key']), unix_time(props['valid']['from']))
                    for props in PropsStream.get(file_name)
                ],
                key = lambda entry: entry.valid_from
            )
        )


    def calib_for(self, file_key):
        key = FileKey.get(file_key)
        valid_from = [ entry.valid_from for entry in self.entries if entry.key.setup == key.setup ]
        pos = bisect.bisect_right(valid_from, key.time)
        if pos > 0:
            return self.entries[pos - 1].key
        else:
            raise RuntimeError("No valid calibration found for file key \"{key}\"".format(key = key.name))
