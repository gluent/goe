#! /usr/bin/env python3
""" Function calls checking orchestration configuration.
    LICENSE_TEXT
"""

import os
import sys


def check_cli_path():
    """Check OFFLOAD_HOME in top level command wrappers
    This should be imported and called as the first GOE import, for example:

    import os

    from goe.config.config_checks import check_cli_path
    check_cli_path()

    import goe.other.libraries.if.required
    """
    if not os.environ.get("OFFLOAD_HOME"):
        print("OFFLOAD_HOME environment variable missing")
        print(
            "You should source environment variables first, eg: . ../conf/offload.env"
        )
        sys.exit(1)
