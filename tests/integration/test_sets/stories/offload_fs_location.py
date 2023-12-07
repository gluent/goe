from .story_setup_functions import *

from goe.filesystem.gluent_dfs import OFFLOAD_FS_SCHEME_S3A, OFFLOAD_FS_SCHEME_HDFS, OFFLOAD_FS_SCHEME_INHERIT, \
    OFFLOAD_FS_SCHEME_MAPRFS, VALID_OFFLOAD_FS_SCHEMES, get_scheme_from_location_uri
from goe.offload.offload_constants import HADOOP_BASED_BACKEND_DISTRIBUTIONS

""" Functions to do some checking related to cloud storage offloads. No specific tests, we piggy back existing
    tests which check data/result set correctness
"""

def get_table_fs_scheme(db_name, table_name, backend_api):
    table_location = backend_api.get_table_location(db_name, table_name)
    return get_scheme_from_location_uri(table_location) if table_location else None


def base_table_fs_scheme_is_correct(data_db, table_name, options, backend_api, option_fs_scheme_override=None):
    """ Ensure the data was offloaded to the correct fs scheme (hdfs vs cloud storage)
        This works on the assumption that "inherit" is the same as "hdfs", in real life
        it may not but in TeamCity it should be a safe assumption
    """
    if not options:
        return True
    if options.backend_distribution not in HADOOP_BASED_BACKEND_DISTRIBUTIONS:
        return True
    expected_scheme = option_fs_scheme_override or options.offload_fs_scheme
    log('base_table_fs_scheme_is_correct(%s, %s)' % (data_db, table_name), detail=verbose)
    scheme = get_table_fs_scheme(data_db, table_name, backend_api)
    log('base_table_fs_scheme_is_correct: %s == %s' % (scheme, expected_scheme), detail=verbose)
    if scheme == expected_scheme:
        return True
    elif scheme in (OFFLOAD_FS_SCHEME_HDFS, OFFLOAD_FS_SCHEME_MAPRFS) and expected_scheme == OFFLOAD_FS_SCHEME_INHERIT:
        return True
    return False


def delta_table_fs_scheme_is_correct(delta_db, table_name, backend_api):
    """ Incr update delta tables should always be in hdfs
    """
    if not backend_api:
        return True
    log('delta_table_fs_scheme_is_correct(%s, %s)' % (delta_db, table_name), detail=verbose)
    scheme = get_table_fs_scheme(delta_db, table_name, backend_api)
    log('delta_table_fs_scheme_is_correct: %s' % scheme, detail=verbose)
    return bool(scheme in (OFFLOAD_FS_SCHEME_HDFS, OFFLOAD_FS_SCHEME_MAPRFS))


def bkup_table_fs_scheme_is_correct(bkup_db, table_name, backend_api):
    """ Incr update delta tables should always be in hdfs
    """
    if not backend_api:
        return True
    log('bkup_table_fs_scheme_is_correct(%s, %s)' % (bkup_db, table_name), detail=verbose)
    scheme = get_table_fs_scheme(bkup_db, table_name, backend_api)
    log('bkup_table_fs_scheme_is_correct: %s' % scheme, detail=verbose)
    return bool(scheme in (OFFLOAD_FS_SCHEME_HDFS, OFFLOAD_FS_SCHEME_MAPRFS))
