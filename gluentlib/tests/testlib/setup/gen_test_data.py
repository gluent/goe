""" Library of functions generating data for test tables.
    shared between legacy 'test --setup' and 'test_setup' scripts.
    LICENSE_TEXT
"""

from datetime import datetime, date, timedelta, time as dt_time
import decimal
import random
import uuid

import cx_Oracle as cxo


###############################################################################
# CONSTANTS
###############################################################################

MAX_DATE_EPOCH_SECS = 2531417560


###############################################################################
# GLOBAL FUNCTIONS
###############################################################################

def remove_newlines(s):
    return s.replace(chr(13), chr(95)).replace(chr(10), chr(95)) if s else s


def gen_char(row_index, length, from_list=None, const=None, ordered=False, ascii7_only=False,
             notnull=False, no_newlines=False):
    if from_list:
        # Just return a value from the list provided by the user
        return from_list[(row_index + 1) % len(from_list)]
    elif const == ordered:
        row = ''.join(chr(row_index % 2 ** 8) for _ in range(length // 2))
    s = ''
    if ordered and ascii7_only and notnull:
        c = (row_index % ((2 ** 7) - 2)) + 1
        s = ''.join(chr(c) for _ in range(random.randint(0, length // 2)))
    elif ordered and ascii7_only:
        c = row_index % ((2 ** 7) - 1)
        s = ''.join(chr(c) for _ in range(length // 2))
    elif ascii7_only and notnull:
        s = ''.join(chr(random.randint(1, 2 ** 7 - 1)) for _ in range(length // 2))
    elif ascii7_only:
        s = ''.join(chr(random.randint(0, 2 ** 7 - 1)) for _ in range(length // 2))
    elif const:
        s = ''.join(const for _ in range(length // 2))
    else:
        s = ''.join(chr(random.randint(0, 2 ** 8 - 1)) for _ in range(length // 2))
    if no_newlines:
        s = remove_newlines(s)
    return s


def gen_varchar(row_index, length, from_list=None, const=None, ordered=False, ascii7_only=False,
                notnull=False, no_newlines=False):
    if from_list:
        assert isinstance(from_list, list)
        # Just return a value from the list provided by the user
        return from_list[(row_index + 1) % len(from_list)]
    if not notnull and row_index % 50 == 0:
        return None
    s = ''
    if ordered and ascii7_only and notnull:
        c = (row_index % ((2 ** 7) - 2)) + 1
        s = ''.join(chr(c) for i in range(random.randint(0, length // 2)))
    elif ordered and ascii7_only:
        c = row_index % ((2 ** 7) - 1)
        s = ''.join(chr(c) for i in range(random.randint(0, length // 2)))
    elif ordered:
        s = ''.join(chr(row_index % 2 ** 8) for _ in range(random.randint(0, length // 2)))
    elif ascii7_only and notnull:
        s = ''.join(chr(random.randint(1, 2 ** 7 - 1)) for _ in range(random.randint(0, length // 2)))
    elif ascii7_only:
        s = ''.join(chr(random.randint(0, 2 ** 7 - 1)) for _ in range(random.randint(0, length // 2)))
    elif const:
        s = ''.join(const for _ in range(random.randint(0, length // 2)))
    else:
        if notnull or random.choice(list(range(20))) != 0:  # 5% null
            s = ''.join(chr(random.randint(0, 2 ** 8 - 1)) for _ in range(random.randint(0, length // 2)))
    if no_newlines:
        s = remove_newlines(s)
    return s


def gen_bytes(row_index, length):
    if row_index % 5 == 0:
        return None
    return gen_char(row_index, length).encode()


def gen_uuid(row_index):
    if row_index % 5 == 0:
        return None
    return uuid.uuid1().bytes


def gen_number(row_index, precision=None, scale=0, from_list=None, ordered=False):
    if from_list:
        # Just return a value from the list provided by the user
        return from_list[(row_index + 1) % len(from_list)]
    elif ordered:
        return int(row_index)
    elif precision:
        # This should really be 38 but cx-Oracle gives us trouble and 'test' had precision capped at 35
        max_precision = 35
        if scale is None:
            scale = 0
        if precision > max_precision:
            # The requested precision for test data is beyond that supported by Oracle so we need to reduce it.
            if scale > 0:
                # Drop scale by the same proportion as we're dropping precision
                scale = int(scale - (scale * (precision - max_precision) / precision))
            precision = max_precision

        precision_string = str(random.randint(0, (10**(precision - scale))-1)).zfill(precision - scale)
        scale_string = str(random.randint(0, (10**scale) - 1)).zfill(scale)
        sign = random.choice([1, -1])
        if sign == -1 and precision == 38 and len(precision_string + scale_string) >= 38:
            # GOE-1648, hitting same issue as discussed on GOE-1503:
            #   cx_Oracle 7.3.0 issue with generating long negative integers in Python and binding to Oracle NUMBER.
            #   cx_Oracle is mangling the long to cx_Oracle.NUMBER conversion for 38 digit negative numbers and
            #   loading garbage into the Oracle column.
            # Therefore trimming these to 36 digits here
            precision_string = precision_string[:-2]
        if scale == 0:
            result = decimal.Decimal(precision_string) * sign
        else:
            # float is painful so we use Decimal below for accuracy
            result = decimal.Decimal(precision_string + '.' + scale_string) * sign
        return result
    else:
        # Because of decimal(, 18), just hack for now
        return round(float(str(random.uniform(-1, 1))[:10]) * (10**random.randint(-4, 4)), 18)
        # return random.uniform(-1, 1) * 10**random.randint(-129, 125)


def gen_int(row_index, precision=None, from_list=None, ordered=False):
    return gen_number(row_index, precision=precision, scale=0, from_list=from_list, ordered=ordered)


def gen_float(row_index, from_list=None, allow_nan=True, allow_inf=True):
    if from_list:
        # Just return a value from the list provided by the user
        return from_list[(row_index + 1) % len(from_list)]
    if row_index % 100 == 0 and allow_nan:
        return float('nan')
    if row_index % 101 == 0 and allow_inf:
        return float('inf')
    if row_index % 50 == 0:
        return None
    # normally distributed exponents
    e = min(125, max(-125, int(random.gauss(0, 25))))
    return random.uniform(-10, 10) * 2**e


def gen_datetime(row_index, scale=6, from_list=None, ordered=False):
    if from_list:
        # Just return a value from the list provided by the user
        return from_list[(row_index + 1) % len(from_list)]
    elif ordered:
        return datetime.fromtimestamp(row_index)
    elif scale == 0:
        return datetime.fromtimestamp(random.randint(0, MAX_DATE_EPOCH_SECS)).replace(microsecond=0)
    else:
        return datetime.fromtimestamp(random.randint(0, MAX_DATE_EPOCH_SECS))


def gen_date(row_index, from_list=None, ordered=False):
    # A true date with no time element
    if from_list:
        # Just return a value from the list provided by the user
        return from_list[(row_index + 1) % len(from_list)]
    elif ordered:
        return date.fromtimestamp(row_index)
    else:
        return date.fromtimestamp(random.randint(0, MAX_DATE_EPOCH_SECS))


def gen_timestamp(row_index, scale=6, from_list=None, ordered=False):
    if from_list:
        # Just return a value from the list provided by the user
        return from_list[(row_index + 1) % len(from_list)]
    elif ordered:
        # Spacing ordered timestamps by 8 hours, to facilitate TZ tests
        return datetime.fromtimestamp((row_index * (60 * 60 * 8)) + random.randint(-3600, 3600) + random.random())
    elif scale == 0:
        return datetime.fromtimestamp(random.randint(0, MAX_DATE_EPOCH_SECS)).replace(microsecond=0)
    else:
        return datetime.fromtimestamp(random.randint(0, MAX_DATE_EPOCH_SECS) + random.random())


def gen_time(row_index, scale=6, from_list=None, ordered=False):
    # A true time with no date element
    if from_list:
        # Just return a value from the list provided by the user
        return from_list[(row_index + 1) % len(from_list)]
    elif ordered:
        return (datetime.min + timedelta(seconds=row_index)).time()
    elif scale == 0:
        return (datetime.min + timedelta(seconds=random.randint(1, (60 * 60 * 24)) - 1)).time().replace(microsecond=0)
    else:
        return (datetime.min + timedelta(seconds=random.randint(1, (60 * 60 * 24)) - 1)).time()


def gen_interval_ym(precision=9):
    precision_string = '999999999'[:precision]
    return "%s%s-%02d" % (random.choice(['', '-']), random.randrange(int(precision_string)), random.randint(0, 11))


def gen_interval_ds(precision=9, scale=9):
    precision_string = '999999999'[:precision]
    scale_string = '999999999'[:scale]
    return "%s%s %02d:%02d:%02d.%s" % (random.choice(['', '-']), random.randrange(int(precision_string)),
                                 random.randint(0, 23), random.randint(0, 59), random.randint(0, 59),
                                 random.randrange(int(scale_string)))


def gen_cxo_type_spec(col_type, length=None, precision=None, scale=None):
    if 'CHAR' in col_type:
        return length
    #GOE-1503: Following workaround now disabled as we've temporarily shrunk GL_TYPES.NUMBER_16 to a NUMBER(36)
    #elif col_type == 'NUMBER' and precision == 38 and scale in [0, None]: # workaround for cx_Oracle 7.3.0 putting signed integers of 38 digits in as garbage
    #    return cxo.STRING
    elif col_type in ('NUMBER', 'FLOAT'):
        return cxo.NUMBER
    elif 'DATE' in col_type:
        return cxo.DATETIME
    elif 'TIMESTAMP' in col_type:
        return cxo.TIMESTAMP
    elif col_type in ('BINARY_FLOAT', 'BINARY_DOUBLE'):
        return cxo.NATIVE_FLOAT
    elif col_type == 'BLOB':
        return cxo.BLOB
    elif col_type == 'CLOB':
        return cxo.CLOB
    elif col_type == 'NCLOB':
        return cxo.NCLOB
    elif col_type == 'RAW':
        return cxo.BINARY
