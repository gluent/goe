"""
LICENSE_TEXT
"""

try:
    from packaging.version import Version as GOEVersion
except ModuleNotFoundError:
    from distutils.version import LooseVersion as GOEVersion
