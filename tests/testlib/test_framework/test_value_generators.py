#! /usr/bin/env python3

# Copyright 2016 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Function to generate test values, e.g. a decimal of precision/scale p, s.
"""

from decimal import Decimal
import random


def decimal_string(p, s, ch="9", cx_safe=False):
    assert p
    if cx_safe and s == 0:
        # GOE-1648, hitting same issue as discussed on GOE-1503:
        #   cx_Oracle 7.3.0 issue with generating long negative integers in Python and binding to Oracle NUMBER.
        #   cx_Oracle is mangling the long to cx_Oracle.NUMBER conversion for 38 digit negative numbers and
        #   loading garbage into the Oracle column.
        # Therefore trimming these to 36 digits here
        p = min(p, 36)
    str_num = ""
    if s:
        str_num = "." + ch.ljust(s, ch)
        # Bump up p to account for decimal place
        p += 1
    return str_num.rjust(p, ch)


class TestDecimal(object):
    """Generate a Python Decimal"""

    @classmethod
    def max(cls, p, s=0):
        """Generate a Python Decimal at the upper limit of precision/scale"""
        return Decimal(decimal_string(p, s))

    @classmethod
    def min(cls, p, s=0):
        """Generate a Python Decimal at the lower limit of precision/scale"""
        return Decimal("-" + decimal_string(p, s, cx_safe=True))

    @classmethod
    def rnd(cls, p, s=0):
        assert p
        random_dec = random.randint(0, (10 ** (p - s)) - 1)
        integral_part = random_dec
        if s:
            decimal_part = random.randint(0, (10**s) - 1)
            return Decimal("%d.%s" % (integral_part, str(decimal_part).zfill(s)))
        else:
            return Decimal(str(integral_part))
