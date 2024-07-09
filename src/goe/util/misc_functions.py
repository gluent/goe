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

""" Miscellaneous useful functions that do not fit anywhere else
"""

import decimal
import datetime
import getpass
import inspect
import logging
import math
import os
import random
import re
import string
import sys
from typing import Optional, Union
import uuid

from base64 import b64encode
from copy import copy
from dateutil import parser
from hashlib import md5


###############################################################################
#  CONSTANTS
###############################################################################

# Python datatype recognition regexes for parse_python_from_string()
REGEX_TRUE = re.compile("True", re.I)
REGEX_FALSE = re.compile("False", re.I)
REGEX_NONE = re.compile("None", re.I)

rdbms_max_descriptor_length = 30

LINUX_FILE_NAME_LENGTH_LIMIT = 255

MAX_SUPPORTED_PRECISION = 38


def all_int_chars(int_str, allow_negative=False):
    """returns true if all chars in a string are 0-9"""
    if allow_negative:
        return bool(re.match(r"^-?\d+$", str(int_str)))
    else:
        return bool(re.match(r"^\d+$", str(int_str)))


def end_by(s, ch):
    """Make sure that character: 'ch' is at the end of string: 's'"""
    if s.endswith(ch):
        return s
    else:
        return s + ch


def begin_with(s, ch):
    """Make sure that character: 'ch' is at the beginning of string: 's'"""
    if s.startswith(ch):
        return s
    else:
        return ch + s


def surround(s, ch):
    """Surround string 's' with 'ch' before and after
    i.e. my/file + '/' -> /my/file/
         /directory/file -> /directory/file/
    """
    return end_by(begin_with(s, ch), ch)


def unsurround(s, ch, endch=None):
    """Unsurround string 's' from being encolsed by 'ch'
        e.g. unsurround('"1,2"', '"') -> '1,2'
    Accepts optional endch which then turns ch into a start ch:
        e.g. unsurround('(1,2)', '(', ')') -> '1,2'
    """
    first_ch = ch
    last_ch = endch or ch
    if s and s.startswith(first_ch) and s.endswith(last_ch):
        return s[1:-1]
    else:
        return s


def chunk_list(lst, chunk_size):
    """Chunk list into batches of chunk_size"""
    for i in range(0, len(lst), chunk_size):
        yield lst[i : i + chunk_size]


def set_goelib_logging(log_level, extra_loggers=None):
    """Set "global" logging parameters and exclude all non-goelib loggers"""
    if not extra_loggers:
        extra_loggers = []

    # Disable library loggers
    for logger_name in logging.Logger.manager.loggerDict:
        if (
            logger_name != "__main__"
            and logger_name not in extra_loggers
            and not logger_name.startswith("goelib")
        ):
            logging.getLogger(logger_name).level = logging.CRITICAL

    logging.basicConfig(
        level=logging.getLevelName(log_level),
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_python_from_string(value):
    """Translate string 'value' into Python datatype
    based on 'appearance'
    """
    assert isinstance(value, str)

    if is_number(value):
        value = float(value) if "." in value else int(value)
    elif REGEX_TRUE.match(value):
        value = True
    elif REGEX_FALSE.match(value):
        value = False
    elif REGEX_NONE.match(value):
        value = None
    else:
        # datetime ?
        try:
            value = parser.parse(value)
        except ValueError:
            pass

    return value


def csv_split(csv: str, sep=",") -> list:
    """Trivial function that splits a CSV and strip()s the tokens"""
    return [_.strip() for _ in csv.split(sep)]


def csvkv_to_dict(csvkv, sep=",", pythonize=True) -> dict:
    """Transform: key1=value1, key2=value2,...
    to: {key1: value1, key2:value2, ...}
    """
    ret = {}

    for kv_chunk in csvkv.split(sep):
        key, value = csv_split(kv_chunk, sep="=")

        ret[key] = parse_python_from_string(value) if pythonize else value

    return ret


def split_not_in_quotes(to_split: str, sep=" ", exclude_empty_tokens=False) -> list:
    """Split a string by a separator but only when the separator is not inside quotes.
    re pattern taken from this comment:
        https://stackoverflow.com/a/2787979/10979853
    The commenter's words should the link ever go stale:
        Each time it finds a semicolon, the lookahead scans the entire remaining string,
        making sure there's an even number of single-quotes and an even number of double-quotes.
        (Single-quotes inside double-quoted fields, or vice-versa, are ignored.) If the
        lookahead succeeds, the semicolon is a delimiter.
    The pattern doesn't cope with whitespace as sep, back to back spaces are multiple seps, therefore
    we have exclude_empty_tokens parameter.
    """
    pattern = r"""%(sep)s(?=(?:[^'"]|'[^']*'|"[^"]*")*$)""" % {"sep": sep}
    if exclude_empty_tokens:
        return [t for t in re.split(pattern, to_split) if t]
    else:
        return re.split(pattern, to_split)


def timedelta_to_str(td):
    """Convert timedelta to d H:M:S string
    (why is this not a part of standard timedelta ?)
    """
    hours, remainder = divmod(td.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    return "%s%02d:%02d:%02d" % (
        str(td.days) if td.days > 0 else "",
        hours,
        minutes,
        seconds,
    )


def is_number(s):
    """Return True if string: 's' can be converted to a number, False otherwise"""
    if s is None:
        return False

    # For some strange reason, boolean is 'int' in python
    # i.e. float(True) = 1.0 - we do not want that
    if type(s) in (type(True), type(False)):
        return False

    try:
        float(s)
        return True
    except (ValueError, TypeError):
        return False


def is_pos_int(val, allow_zero=False):
    """Return True/False based on whether incoming value is a positive integer"""
    lower_bound = 0 if allow_zero else 1
    try:
        n = int(val)
        if n < lower_bound:
            return False
        if decimal.Decimal(val) != n:
            return False
        return True
    except (TypeError, ValueError):
        return False


def is_power_of_10(n):
    """Return True if the input is a power of 10, e.g.: 100, 100000, etc"""
    if isinstance(n, str):
        n = decimal.Decimal(n)
    return bool(n and (n / 10 ** (len(str(n)) - 1)) == 1)


def truncate_number(n, digits_to_keep=0):
    """Truncate a number to a specific number of decimal places, mimic TRUNC in Impala/BigQuery.
    Truncate rounds towards zero (not -inf).
    digits_to_keep has two forms
        >= 0: Truncate decimal places, e.g. truncate_number(123.456, 1) == 123.4
        < 0: Additionally truncate digits to left of decimal place, e.g. truncate_number(123.456, -1) == 120
    """

    def remove_exponent(d):
        """Taken from Decimal official FAQ"""
        return d.quantize(decimal.Decimal(1)) if d == d.to_integral() else d.normalize()

    if not isinstance(n, decimal.Decimal):
        num = decimal.Decimal(str(n))
    else:
        num = n
    original_context = decimal.getcontext()
    # Ensure we have enough headroom to multiply by digits_to_keep
    tn_context = decimal.Context(prec=MAX_SUPPORTED_PRECISION + abs(digits_to_keep) + 1)
    decimal.setcontext(tn_context)
    multiplier = tn_context.power(10, digits_to_keep)
    n_multiplied = (num * multiplier).to_integral_value(rounding=decimal.ROUND_DOWN)
    if n_multiplied == 0:
        n_final = decimal.Decimal("0")
    else:
        n_final = remove_exponent(n_multiplied / multiplier)
    decimal.setcontext(original_context)
    if isinstance(n, int):
        return int(n_final)
    elif isinstance(n, float):
        return float(n_final)
    else:
        return n_final


def nvl(val, repl):
    """If 'val' is "not set", replace it with 'repl'"""
    return val or repl


def backtick_sandwich(s, ch="`"):
    """Return a copy of the given string, sandwiched with a single pair of backticks
    For example:
    backtick_sandwich("foo") => "`foo`"
    backtick_sandwich("`foo") => "`foo`"
    backtick_sandwich("foo`") => "`foo`"
    backtick_sandwich("`foo`") => "`foo`"
    backtick_sandwich("``foo``") => "`foo`"
    Note: strip() only removes ch from the outer edges of s. This behaviour is relied upon
          in some cases, e.g. backtick_sandwich('`sh`.`table`') will retain the inner backticks
          and give correct results. Beware changing this behaviour!
    """
    return ch + s.strip(ch) + ch


def double_quote_sandwich(s):
    return backtick_sandwich(s, '"')


def isclose(a, b, rel_tol=1e-09, abs_tol=0.0):
    """Are two numbers (i.e. floats) 'close enough' to be considered 'the same' ?"""
    return abs(a - b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)


def typed_property(name, expected_type):
    """Define 'property' in a class
    (to reduce @property/@prop.setter boilerplate code)

    To use, define properties in class constructor as:
        name = typed_property('name', str)
        executed = typed_property('executed', int)
    """
    storage_name = "_" + name

    @property
    def prop(self):
        return getattr(self, storage_name)

    @prop.setter
    def prop(self, value):
        if not isinstance(value, expected_type):
            raise TypeError("%s must be a %s" % (name, str(expected_type)))
        setattr(self, storage_name, value)

    return prop


def get_option(options, name, repl=None):
    """Return options 'name' attribute if it exists, 'repl' otherwise

    Similar behavior to: dict.get()
    """
    return getattr(options, name) if (options and hasattr(options, name)) else repl


def str_floatlike(maybe_float):
    """Remove unnecessary 0s from the float or 'float like string'
    Warning: Relies on str() conversion for floats, which truncates floats
             with >11 digits after '.'
    """
    if is_number(maybe_float) and not float(maybe_float).is_integer():
        return str(maybe_float).rstrip("0").rstrip(".")
    else:
        return maybe_float


def trunc_with_hash(
    s, hash_length, max_length, force_append_hash=False, hash_case_conv_fn=None
):
    """Truncate a string with a suffix of a hash of its original value so the final length is max_length"""
    assert s
    assert hash_length
    assert max_length
    if hash_case_conv_fn:
        assert callable(hash_case_conv_fn)
    if len(s) <= max_length and not force_append_hash:
        return s
    else:
        stem = s[: max_length - hash_length - 1]
        # hash -> b64 it into chars -> keep identifier chars and trim to hash_length
        minihash = b64encode(md5(s.encode()).digest()).decode()
        minihash = re.sub(r"[\W\s]", "", minihash)[:hash_length]
        if hash_case_conv_fn:
            minihash = hash_case_conv_fn(minihash)
        return stem + "_" + minihash


def standard_file_name(
    file_prefix,
    name_suffix="",
    extension="",
    max_name_length=LINUX_FILE_NAME_LENGTH_LIMIT,
    with_datetime=False,
    max_name_length_extra_slack=0,
    hash_case_conv_fn=None,
):
    """A standard routine to generate a file name.
    Assumptions are:
        The calling code will deal with adding any container path. This is for the file name only.
        If the resulting file name is going to be longer than LINUX_FILE_NAME_LENGTH_LIMIT then it will be trimmed.
        AFTER any LINUX_FILE_NAME_LENGTH_LIMIT truncation:
            This code will append a suffix containing name_suffix
            If with_datetime=True, this code will append a suffix containing the current date/time.
            This code with append an extension.
        This means that a truncated file will still contain any exta suffix or timestamp.
        max_name_length_extra_slack can be used when extra overhead is needed within LINUX_FILE_NAME_LENGTH_LIMIT
        case_conv_fn can be used to upper or lower case the file name prior to adding the extension.
    """
    assert file_prefix
    assert isinstance(file_prefix, str)
    assert "/" not in file_prefix
    assert isinstance(name_suffix, str)
    assert isinstance(extension, str)
    assert isinstance(max_name_length_extra_slack, int)
    if hash_case_conv_fn:
        assert callable(hash_case_conv_fn)

    log_append = ""
    if name_suffix:
        log_append += "_" + name_suffix
    if with_datetime:
        log_append += "_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    over_limit = (
        len(file_prefix)
        + len(log_append)
        + len(extension)
        + max_name_length_extra_slack
    ) - max_name_length
    if over_limit > 0:
        new_log_prefix = trunc_with_hash(
            file_prefix,
            4,
            len(file_prefix) - over_limit,
            hash_case_conv_fn=hash_case_conv_fn,
        )
    else:
        new_log_prefix = file_prefix
    log_name = "%s%s%s" % (new_log_prefix, log_append, extension)
    return log_name


def standard_log_name(log_prefix):
    return standard_file_name(log_prefix, extension=".log", with_datetime=True)


def get_decimal_precision_and_scale(data_type_string):
    """Parse p and s out of DECIMAL(p,s) or DECIMAL(p)"""
    if "," in data_type_string:
        pattern = r"^DECIMAL\s*\(([1-9][0-9]?)\s*,\s*([0-9][0-9]?)\)$"
    else:
        pattern = r"^DECIMAL\s*\(([1-9][0-9]?)\s*\)$"
    m = re.match(pattern, data_type_string, flags=re.IGNORECASE)
    if not m or len(m.groups()) not in (1, 2):
        return None
    precision = int(m.group(1))
    scale = int(m.group(2)) if len(m.groups()) == 2 else 0
    return (precision, scale)


def get_integral_part_magnitude(py_val):
    """Gets the magnitude of the integral part of a number
    Lifted from:
    https://stackoverflow.com/questions/3018758/determine-precision-and-scale-of-particular-number-in-python
    """
    int_part = int(abs(py_val))
    magnitude = 1 if int_part == 0 else int(math.log10(int_part)) + 1
    return magnitude


def bytes_to_human_size(size_bytes, scale=1):
    """Translate size from a number to human friendly form, i.e.:
    223 = 223B
    223 *1024 * 1024 = 223MB
    """
    assert size_bytes is not None
    size_str = None
    pattern = "%." + str(scale) + "f"

    if size_bytes < 1024:
        size_str = "%dB" % size_bytes
    elif size_bytes < 1024 * 1024:
        size_str = (pattern % round(size_bytes / 1024.0, scale)) + "KB"
    elif size_bytes < 1024 * 1024 * 1024:
        size_str = (pattern % round(size_bytes / 1024.0 / 1024, scale)) + "MB"
    elif size_bytes < 1024 * 1024 * 1024 * 1024:
        size_str = (pattern % round(size_bytes / 1024.0 / 1024 / 1024, scale)) + "GB"
    else:
        size_str = (
            pattern % round(size_bytes / 1024.0 / 1024 / 1024 / 1024, scale)
        ) + "TB"

    return size_str


def human_size_to_bytes(size, binary_sizes=True):
    """Translate size from a human friendly form to bytes, i.e.:
    223B = 223
    64K = 64 * 1024
    binary_sizes=False uses 1000 rather than 1024 as the multiplier
    """
    if size is None:
        return size
    elif isinstance(size, bytes):
        size = size.decode()
    elif not isinstance(size, str):
        size = str(size)
    m = re.match(r"^([\d\.]+)([BKMGT])?$", size)
    if not m:
        m = re.match(r"^([\d\.]+)([KMGT]B)?$", size)
    if m:
        if binary_sizes:
            mag_dict = {
                "B": 1,
                "K": 1024,
                "M": 1024**2,
                "G": 1024**3,
                "T": 1024**4,
                "P": 1024**5,
            }
        else:
            mag_dict = {
                "B": 1,
                "K": 10**3,
                "M": 10**6,
                "G": 10**9,
                "T": 10**12,
                "P": 10**15,
            }
        if m.group(2):
            if len(m.group(2)) == 2:
                mag_key = m.group(2)[0]
            else:
                mag_key = m.group(2)
            mag = mag_dict[mag_key]
        else:
            mag = 1
        return int(float(m.group(1)) * mag)
    return None


# not used tempfile.mkstemp for get_temp_path/write_temp_file as sometimes
# want to generate a file name to use remotely or fully control the random section
def get_temp_path(tmp_dir: str = "/tmp", prefix: str = "goe_tmp_", suffix: str = ""):
    suffix_str = (".%s" % suffix.lstrip(".")) if suffix else ""
    return os.path.join(tmp_dir, "%s%s%s" % (prefix, str(uuid.uuid4()), suffix_str))


def write_temp_file(data, prefix: str = "goe_tmp_", suffix: str = ""):
    """Writes some data to a temporary file and returns the path to the file"""
    tmp_path = get_temp_path(prefix=prefix, suffix=suffix)
    with open(tmp_path, "w") as fh:
        fh.write(data)
    return tmp_path


def id_generator(size=8, chars=string.ascii_uppercase + string.digits):
    """Generate a pseudo random list of characters suitable for an ID.
    Lifted straight from stackoverflow:
      https://stackoverflow.com/questions/2257441/random-string-generation-with-upper-case-letters-and-digits-in-python
    """
    return "".join(random.choice(chars) for _ in range(size))


def obscure_list_items(list_of_items, to_obscure):
    """Process a list returning a copy with certain items obscured
    list_of_items: A list, ['this', 'that', '--pwd', 'super-sensitive', 'other']
    to_obscure:  A list of items to obscure, e.g.
                 [{'item': 'super-sensitive', 'prior': '--pwd', 'sub': 'XXXXXX' {...}]
                   'prior' and 'sub' are optional, 'sub' defaults to "?"
    """
    safe_list = copy(list_of_items)
    if not to_obscure:
        return safe_list

    for itm in to_obscure:
        # Obscure the item if it matches the value 'item' and, optionally, the prior item in the list match 'prior'
        safe_list = [
            (
                itm.get("sub", "?")
                if safe_list[i] == itm["item"]
                and (
                    not itm.get("prior")
                    or (i > 0 and itm.get("prior") == safe_list[i - 1])
                )
                else safe_list[i]
            )
            for i in range(len(safe_list))
        ]

    return safe_list


def get_os_username():
    try:
        # Do not use os.getlogin because if the user used sudo it would return the original parent login.
        return getpass.getuser()
    except Exception:
        return os.environ.get("USER")


def ansi_c_string_safe(shell_command):
    """Use "ANSI C like strings" in shell to protect special characters and allow escaping"""
    return "$'%s'" % shell_command.replace("\\", r"\\").replace("'", r"\'")


def match_case(token, affix, position, operation):
    """Join a string to another string or substitute a string but in the same case (upper or lower)."""
    assert token and operation in ("concatenate", "substitute")
    if operation == "concatenate":
        assert position in ("prefix", "suffix") and affix
        if token.isupper():
            return (
                (affix.upper() if position == "prefix" else "")
                + token
                + (affix.upper() if position == "suffix" else "")
            )
        elif token.islower():
            return (
                (affix.lower() if position == "prefix" else "")
                + token
                + (affix.lower() if position == "suffix" else "")
            )
        else:
            # All bets are off
            return (
                (affix if position == "prefix" else "")
                + token
                + (affix if position == "suffix" else "")
            )
    elif operation == "substitute":
        if affix.isupper():
            return (token % affix).upper()
        elif affix.islower():
            return (token % affix).lower()
        else:
            # All bets are off
            return token % affix


def add_suffix_in_same_case(token, suffix):
    """Append a string to another string but in the same case (upper or lower)."""
    assert token and suffix
    return match_case(token, suffix, "suffix", "concatenate")


def add_prefix_in_same_case(token, prefix):
    """Prepend a string to another string but in the same case (upper or lower)."""
    assert token and prefix
    return match_case(token, prefix, "prefix", "concatenate")


def substitute_in_same_case(pattern, token):
    """Substitute a string in a pattern but in the same case (upper or lower)."""
    assert pattern and "%" in pattern
    return match_case(pattern, token, None, "substitute")


def case_insensitive_in(
    token: str, search_list: Union[list, str, set]
) -> Optional[str]:
    """
    Search for token in search_list with an upper() on both sides. Returns the matched value search_list for truthyness.
    """

    def to_upper(s):
        return s if s is None else s.upper()

    if not search_list:
        return None

    if isinstance(search_list, str):
        return search_list if to_upper(token) == to_upper(search_list) else None
    else:
        matches = [_ for _ in search_list if to_upper(token) == to_upper(_)]
        return matches[0] if matches else None


def format_json_list(json_list, separator="\n", indent=0):
    """Formats a JSON list as separated elements with optional indentation"""
    if json_list is None:
        return ""
    safe_to_str = lambda x: x if isinstance(x, str) else str(x)
    sep = separator + (" " * indent)
    return sep.join(safe_to_str(_) for _ in json_list)


def plural(s, n, caps=False):
    """Simple function for logging plurals
    (e.g. "Did something with %s %s" % (n, plural(n, 'table')))
    """
    if s[-1].lower() != "y":
        p = "s"
        ps = "%s%s" % (s, p if n != 1 else "")
    else:
        p = "ies"
        ps = "%s%s" % (s[0:-1] if n != 1 else s, p if n != 1 else "")
    return ps.upper() if caps else ps


def format_list_for_logging(content_list, underline_char="="):
    """Takes a list of tuples and formats them into a string table suitable for logging.
    Numerics are right justified and column width is auto calculated.
    Assumes first line contains headings.
    e.g. input:
        [('Header1', 'LongerHeader'),
         ('Blah', 123)]
    results in:
        Header1 LongerHeader
        ======= ============
        Blah             123
    """

    def width_fn(x):
        return len(x) if isinstance(x, str) else len(str(x))

    def justify_fn(x):
        return "<" if isinstance(x, str) else ">"

    assert content_list
    assert isinstance(content_list, list), "{} is not list".format(type(content_list))
    assert isinstance(content_list[0], tuple)
    widths = []
    for i in range(len(content_list[0])):
        widths.append(max(width_fn(_[i]) for _ in content_list))
    header_format = " ".join(("{%s: <%s}" % (i, w)) for i, w in enumerate(widths))
    table_rows = [
        header_format.format(*content_list[0]).rstrip(),
        header_format.format(*[(underline_char * _) for _ in widths]),
    ]
    if len(content_list) < 2:
        # No table data so just return headings
        return "\n".join(table_rows + ["No data"])
    row_format = " ".join(
        ("{%s: %s%s}" % (i, justify_fn(c), w))
        for i, (w, c) in enumerate(zip(widths, content_list[1]))
    )
    string_table = "\n".join(
        table_rows + [row_format.format(*_).rstrip() for _ in content_list[1:]]
    )
    return string_table


def remove_chars(input_string, chars_to_remove):
    """Remove and characters in chars_to_remove from input_string and return the results"""
    if input_string is None:
        return input_string
    assert isinstance(input_string, str)
    assert isinstance(chars_to_remove, str)
    return "".join(_ for _ in input_string if _ not in chars_to_remove)


def str_summary_of_self(other_self):
    """Helper function to create a string representation of an object, to be used in __str__ methods"""
    assert other_self
    return "({})".format(
        ", ".join(
            "{}={}".format(k, v)
            for k, v in inspect.getmembers(other_self)
            if not inspect.ismethod(v) and not inspect.isfunction(v) and k[0] != "_"
        )
    )


def to_bytes(bytes_or_str):
    if isinstance(bytes_or_str, str):
        return bytes_or_str.encode("utf-8")
    else:
        return bytes_or_str


def to_str(bytes_or_str):
    if isinstance(bytes_or_str, bytes):
        return bytes_or_str.decode("utf-8")
    else:
        return bytes_or_str


def wildcard_matches_in_list(pattern, list_of_names, case_sensitive=True):
    """Return a list of strings in list_of_names that match the simple wildcard pattern.
    pattern only supports '*' as special character.
    """
    if not list_of_names:
        return []
    # Protect against any attempted regex use in pattern
    pattern = re.escape(pattern)
    # Reinstate any '*' wildcards
    pattern = pattern.replace("\\*", ".*")
    # Anchor the pattern
    pattern = r"^{}$".format(pattern)
    pattern_re = re.compile(pattern) if case_sensitive else re.compile(pattern, re.I)
    matches = [_ for _ in list_of_names if pattern_re.match(_)]
    return matches
