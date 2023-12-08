#! /usr/bin/env python3
""" FormatLiteralInterface: Base interface for SQL engine specific implementations to format literals based on data type
    LICENSE_TEXT
"""

from abc import ABCMeta, abstractmethod
import logging

###############################################################################
# CONSTANTS
###############################################################################

RE_TIMEZONE_NO_COLON = r".*(-|\+)(\d{2})(\d{2})"


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default


###########################################################################
# FormatLiteralInterface
###########################################################################


class FormatLiteralInterface(metaclass=ABCMeta):
    """Abstract base class which acts as an interface for backend/frontend specific sub-classes.
    Used to format literals correctly for the specific implementation.
    """

    @abstractmethod
    def format_literal(self, python_value, data_type=None):
        """Return a string containing a correctly formatted literal for data_type.
        Without a data type the code should infer a literal from the Python type.
        """
        pass

    @classmethod
    def _strip_unused_time_scale(cls, str_val, trim_unnecessary_subseconds=False):
        """If there are fractional seconds then remove farthest right zeros because
        Oracle might pass through "2030-01-02 00:00:00.000003000" which a backend
        may reject because of the trailing zeros.
        """
        if "." in str_val:
            new_val = str_val.rstrip("0")
            if new_val[-1] == ".":
                if trim_unnecessary_subseconds:
                    new_val = new_val[:-1]
                else:
                    new_val += "0"
            return new_val
        else:
            return str_val
