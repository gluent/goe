#! /usr/bin/env python3
""" config_descriptions: Library of constants defining descriptions for configuration attributes
    In the future we expect to refactor all option processing, including descriptions, and this module will
    hopefully become redundant at that time.
    LICENSE_TEXT
"""

DATA_SAMPLE_PARALLELISM = "Degree of parallelism to use when sampling RDBMS data for columns with no precision/scale properties. Values of 0 or 1 will execute the query without parallelism"

VERIFY_PARALLELISM = "Degree of parallelism to use for the RDBMS query executed when validating an offload. Values of 0 or 1 will execute the query without parallelism. Values > 1 will force a parallel query of the given degree. If unset, the RDBMS query will fall back to using the behavior specified by RDBMS defaults"
