# Copyright 2020 Nexenta by DDN, Inc. All rights reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import re
import uuid

from oslo_utils import units
import six

from cinder.i18n import _
from cinder import utils as cinder_utils


def str2size(s, scale=1024):
    """Convert size-string.

    String format: <value>[:space:]<B | K | M | ...> to bytes.

    :param s: size-string
    :param scale: base size
    """
    if not s:
        return 0

    if isinstance(s, six.integer_types):
        return s

    match = re.match(r'^([\.\d]+)\s*([BbKkMmGgTtPpEeZzYy]?)', s)
    if match is None:
        raise ValueError(_('Invalid value: %(value)s')
                         % {'value': s})

    groups = match.groups()
    value = float(groups[0])
    suffix = groups[1].upper() if groups[1] else 'B'

    types = ('B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y')
    for i, t in enumerate(types):
        if suffix == t:
            return int(value * pow(scale, i))


def str2gib_size(s):
    """Covert size-string to size in gigabytes."""
    size_in_bytes = str2size(s)
    return size_in_bytes // units.Gi


def native_string(text):
    """Convert to native string.

    Convert bytes and Unicode strings to native strings:

    * convert to bytes on Python 2:
      encode Unicode using encodeutils.safe_encode()
    * convert to Unicode on Python 3: decode bytes from UTF-8
    """
    return cinder_utils.convert_str(text)


def divup(numerator, denominator):
    return (numerator + denominator - 1) // denominator


def roundup(numerator, denominator):
    return divup(numerator, denominator) * denominator


def roundgb(numerator):
    return roundup(numerator, units.Gi) // units.Gi


def match_template(template, name):
    if not (name and isinstance(name, six.string_types) and
            template and isinstance(template, six.string_types)):
        return False
    pattern = template.replace('%s', r'(?P<uuid>.+)')
    if pattern == template:
        return False
    regex = re.compile(pattern)
    match = regex.match(name)
    if match is None:
        return False
    result = match.group('uuid')
    if not (result and isinstance(result, six.string_types)):
        return False
    try:
        uuid.UUID(result, version=4)
    except ValueError:
        return False
    return True


def prt2tpg(portals):
    """Convert list of dicts to list.

    :param portals: list of portals dicts
    :returns flat list of portals
    """
    tpg = []
    for portal in portals:
        hostport = '%(host)s:%(port)s' % {
            'host': portal['address'],
            'port': portal['port']
        }
        tpg.append(hostport)
    return tpg
