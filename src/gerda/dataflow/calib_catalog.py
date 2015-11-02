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
        entries = {}

        for props in PropsStream.get(file_name):
            file_key = FileKey.get(props['key'])
            for system, timestamp in props['valid'].iteritems():
                if system not in entries:
                    entries[system] = []
                entries[system].append(CalibCatalog.Entry(file_key, unix_time(timestamp)))

        for system in entries:
            entries[system] = sorted(
                entries[system],
                key = lambda entry: entry.valid_from
            )

        return CalibCatalog(entries)


    def calib_for(self, file_key, system, allow_none = False):
        if system in self.entries:
            key = FileKey.get(file_key)
            valid_from = [ entry.valid_from for entry in self.entries[system] if entry.key.setup == key.setup ]
            pos = bisect.bisect_right(valid_from, key.time)
            if pos > 0:
                return self.entries[system][pos - 1].key
            else:
                if allow_none: return None
                else: raise RuntimeError("No valid calibration found for file key \"{key}\", system \"{system}\"".format(key = key.name, system = system))
        else:
            if allow_none: return None
            else: raise RuntimeError("No calibrations found for system \"{system}\"".format(system = system))
