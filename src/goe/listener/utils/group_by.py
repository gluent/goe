"""
Utility library.
LICENSE_TEXT
"""

from collections import defaultdict
from functools import reduce  # import needed for python3; builtin in python2


def groupby(key, seq):
    return reduce(
        lambda grp, val: grp[key(val)].append(val) or grp, seq, defaultdict(list)
    )
