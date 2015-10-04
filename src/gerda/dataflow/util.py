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
import dateutil.parser

import arrow


def ensure_str(value):
    if isinstance(value, str):
        return value
    if isinstance(value, unicode):
        return value.encode('utf-8')
    else:
        raise ValueError("Expected string value instead of type {t}".format(t = type(value)))


def ensure_bool(value):
    if isinstance(value, bool):
        return value
    else:
        raise ValueError("Expected bool value instead of type {t}".format(t = type(value)))


def ensure_int(value):
    if isinstance(value, int):
        return value
    else:
        raise ValueError("Expected int value instead of type {t}".format(t = type(value)))


def non_unicode(value):
    if isinstance(value, unicode):
        return value.encode('utf-8')
    else:
        return value


def unix_time(value):
    if isinstance(value, basestring):
        return arrow.get(dateutil.parser.parse(value)).timestamp
    else:
        raise ValueError("Can't convert type {t} to unix time".format(t = type(value)))


def utc_time_str(value):
    if isinstance(value, (types.IntType, types.LongType)):
        return arrow.get(value).to('UTC').format('YYYYMMDDTHHmmss')+'Z'
    else:
        raise ValueError("Can't convert type {t} to ISO 8601 UTC time string".format(t = type(value)))
