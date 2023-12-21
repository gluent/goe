#! /usr/bin/env python3
""" offload_logic: Compare SRC and DST "storage objects" and generate symmetric difference

    LICENSE_TEXT
"""

import logging
import re

from itertools import groupby

from goe.util.config_file import GOERemoteConfig, SECTION_TYPE_S3, SECTION_TYPE_HDFS
from goe.util.misc_functions import end_by, to_str

from goe.cloud.s3_store import S3Store
from goe.cloud.hdfs_store import HdfsStore


###############################################################################
# EXCEPTIONS
###############################################################################
class OffloadLogicException(Exception): pass


###############################################################################
# CONSTANTS
###############################################################################

# Source/destination types
DST_S3 = SECTION_TYPE_S3
DST_HDFS = SECTION_TYPE_HDFS


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###############################################################################
# CLASS: XferSetupRaw
###############################################################################
class XferSetupRaw(object):
    """ XferSetupRaw: Prepare transfer between 'source' and 'target'

           - Query contents
           - Calculate statistics (size, # of files, last_modified etc)
           - Calculate 'offloadable' difference
    """

    def __init__(self, source, target, lite=False):
        """ CONSTRUCTOR

            source and target are "storage driver" objects

        """
        assert source and target

        # Access objects
        self._source = source      # 'Source' driver object
        self._target = target      # 'Destination' driver object

        # Initialize internal structures
        self._initialize(lite)

        logger.debug("XferSetupRaw() object initialized")

    ###########################################################################
    # PRIVATE ROUTINES
    ###########################################################################

    def _initialize(self, lite):
        """ Initialize internal structures (before the new evaluation)
        """
        # Source/target RAW data
        self._source_data = None # Source 'data'
        self._target_data = None # Target 'data'

        self._search_conditions = []      # Data filters
        self._lite = lite                 # True = "Lite" execution mode (aka: give me 'source' data afahp)

        # Module outcome
        self._offloadable_difference = {}  # 'Offloadable' file difference between source and target
        self._offloadable_stats = {}       # 'Offloadable difference' 'statistics'

        self._deletable_difference = {}    # 'Deletable' file difference between source and target
        self._deletable_stats = {}         # 'Deletable difference' 'statistics'

        self._source_stats = {}            # Source 'statistics'
        self._target_stats = {}            # Target 'statistics'

        self._the_same = 0                 # Number of 'the same' files between destinations

    def _key_by_key_name(self, item_list):
        """ Transform: [{'key_name1': ..., ...}, {'key_name2': ..., ...}]
            into: {'key_name1': {'key_name1': ..., ...}, 'key_name2': {'key_name2': ..., ...}}
        """
        ret = {}
        for fl in item_list:
            key_name = fl['key_name']
            ret[key_name] = fl

        return ret

    def _calculate_offloadable_difference(self, source_data, target_data):
        """ Calculate offloadable file difference between source and target
            by finding missing keys and analyzing file sizes for existing keys
        """

        source = self._key_by_key_name(source_data)
        target = self._key_by_key_name(target_data)

        result = []

        for s in source:
            if s not in target or source[s]['size'] != target[s]['size']:
                source[s]['name'] = end_by(self.source_url, '/') + s
                target_s = target[s] if s in target else {'last_modified': None}
                target_s['name'] = end_by(self.target_url, '/') + s
                result.append(
                    {
                        'source': source[s],
                        'target': target_s
                    }
                )
            else:
                self._the_same += 1

        logger.debug("Calculated offloadable file difference: %s" % result)
        return result

    def _calculate_deletable_difference(self, source_data, target_data):
        """ Calculate 'deletable' file difference:
                Files that exist on TARGET but not on SOURCE
        """

        source = self._key_by_key_name(source_data)
        target = self._key_by_key_name(target_data)

        result = []

        for s in target:
            if s not in source:
                target_s = target[s]
                target_s['name'] = end_by(self.target_url, '/') + s
                source_s = {'name': end_by(self.source_url, '/') + s}
                result.append(
                    {
                        'target': target_s,
                        # This is a fake, since source file does not exist,
                        # but we'll need it later to calculate distcp commands
                        'source': source_s
                    }
                )

        logger.debug("Calculated deletable file difference: %s" % result)
        return result

    def _calculate_offloadable_stats(self, data):
        """ Calculate 'offload difference' stats
            data = structure returned by self._calculate_offloadable_difference()
        """

        source_last_modified = self._last_modified_from_data(data, sub_key='source')
        target_last_modified = self._last_modified_from_data(data, sub_key='target')
        result = {
            'size': sum(_['source']['size'] for _ in data),
            'files': len(data),
            'last_modified_on_source': source_last_modified,
            'last_modified_on_target': target_last_modified
        }

        logger.debug("Calculated offloadable difference stats: %s" % result)
        return result

    def _calculate_deletable_stats(self, data):
        """ Calculate 'deletable difference' stats

            data = structure returned by self._calculate_deletable_difference()
        """
        target_last_modified = self._last_modified_from_data(data, sub_key='target')
        target_first_modified = self._last_modified_from_data(data, sub_key='target', min_last_modified=True)
        result = {
            'size': sum(_['target']['size'] for _ in data),
            'files': len(data),
            'last_modified': target_last_modified,
            'first_modified': target_first_modified
        }

        logger.debug("Calculated deletable difference stats: %s" % result)
        return result

    def _calculate_stats(self, data, data_type):
        """ Calculate 'offload data' stats:
                - Number of files
                - Total size
                - (max) last_modified
        """
        result = {
            'size': sum(_['size'] for _ in data),
            'files': len(data),
            'last_modified': self._last_modified_from_data(data)
        }

        logger.debug("Calculated '%s' stats: %s" % (data_type, result))
        return result

    def _evaluate(self, search_conditions, lite):
        """ Actual 'offloadable difference' evaluation
        """
        # Initialize internal structures
        self._initialize(lite)

        # Saving it as everything (source_date, target_date etc) is dependent on it
        self._search_conditions = search_conditions
        self._lite = lite

        src_data_access = self._source.stream if self._lite else self._source.data

        self._source_data = self._source.jmespath_search(where=search_conditions, data=src_data_access) \
            if search_conditions else src_data_access
        if self._lite:
            self._target_data = []
        else:
            self._target_data = self._target.jmespath_search(where=search_conditions, data=self._target.data) \
                if search_conditions else self._target.data

        if not self._lite:
            self._offloadable_difference = self._calculate_offloadable_difference(self._source_data, self._target_data)
            self._deletable_difference = self._calculate_deletable_difference(self._source_data, self._target_data)

            self._source_stats = self._calculate_stats(self._source_data, 'source')
            self._target_stats = self._calculate_stats(self._target_data, 'target')

            self._offloadable_stats = self._calculate_offloadable_stats(self._offloadable_difference)
            self._deletable_stats = self._calculate_deletable_stats(self._deletable_difference)

    def _last_modified_from_data(self, data, sub_key=None, min_last_modified=False):
        def get_last_modified(list_item):
            if sub_key:
                return list_item[sub_key]['last_modified']
            else:
                return list_item['last_modified']
        last_modified = [to_str(get_last_modified(_))
                         for _ in data
                         if get_last_modified(_) is not None] if data else None
        if min_last_modified:
            return min(last_modified) if last_modified else None
        else:
            return max(last_modified) if last_modified else None

    def _prepare_distcp_difference(self, difference):
        """ Return 'distcp' difference:

	    ([source1, source2, ...], target), where:
	        'target' is stripped '2 directories up' (typically, to 'offload_bucket')
	        'source' is stripped '1 directory up' (typicallly, to 'timestamp')
	    	     and then 'grouped by' target

	    This is a bit of a hack
	    to force semi-efficient distcp sync of only relevant files,
	    while not making 'single file' distcp runs
        """
        if not difference:
            logger.debug("Prepared DISTCP file difference is EMPTY")
            return []

        intermediate = []

        for item in difference:
            new_source = '/'.join(item['source']['name'].split('/')[:-1])
            new_target = '/'.join(item['target']['name'].split('/')[:-2])
            intermediate.append((new_source, new_target))

        # Sort by target to facilitate GROUP BY on the next step
        intermediate.sort(key=lambda x: x[1])

        # Group by 'target' and save ([list of source keys], target)
        result = []
        for t, s in groupby(intermediate, key=lambda x: x[1]):
            result.append(([_[0] for _ in s], t))

        logger.debug("Prepared DISTCP file difference: %s" % result)

        return result

    ###########################################################################
    # PROPERTIES
    ###########################################################################

    @property
    def source_drv(self):
        """ Source driver object """
        return self._source

    @property
    def target_drv(self):
        """ Target driver object """
        return self._target

    @property
    def source_all(self):
        """ Return all 'source files' """
        if self._lite:
            return self._source_data
        else:
            return sorted(self._source_data, key=lambda x: x['key_name'])

    @property
    def target_all(self):
        """ Return all 'target files' """
        if self._lite:
            logger.warn("XferSetupRaw.target_all() is undefined in 'lite' mode.")
            return []
        else:
            return sorted(self._target_data, key=lambda x: x['key_name'])

    @property
    def the_same(self):
        """ Number of files that are 'the same' between 'source' and 'target' """
        return self._the_same

    @property
    def offloadable(self):
        """ Return 'source/target names' offloadable difference """
        if self._lite:
            logger.warn("XferSetupRaw.offloadable() is undefined in 'lite' mode.")
            return []
        else:
            return sorted([(_['source']['name'], _['target']['name'])
                           for _ in self._offloadable_difference],
                          key=lambda x: x[0])

    @property
    def offloadable_source(self):
        """ Return 'source files' offloadable difference """
        if self._lite:
            logger.warn("XferSetupRaw.offloadable_source() is undefined in 'lite' mode.")
            return []
        else:
            return sorted([_['source'] for _ in self._offloadable_difference],
                          key=lambda x: x['key_name'])

    @property
    def offloadable_files(self):
        """ Return 'source files' offloadable difference """
        if self._lite:
            logger.warn("XferSetupRaw.offloadable_files() is undefined in 'lite' mode.")
            return []
        else:
            return sorted([_['source']['name'] for _ in self._offloadable_difference])

    @property
    def offloadable_keys(self):
        """ Return 'source keys' offloadable difference """
        if self._lite:
            logger.warn("XferSetupRaw.offloadable_keys() is undefined in 'lite' mode.")
            return []
        else:
            return sorted([_['source']['key_name'] for _ in self._offloadable_difference])

    @property
    def offloadable_distcp(self):
        """ Return offloadable difference suitable to 'file tree' distcp copies

            Specifically, a list of:
            ([source key1, source key2, ...], target) tuples
        """
        if self._lite:
            logger.warn("XferSetupRaw.offloadable_distcp() is undefined in 'lite' mode.")
            return []
        else:
            return self._prepare_distcp_difference(self._offloadable_difference)

    @property
    def offloadable_extended(self):
        """ Return all collected offloadable difference (names, sizes etc) """
        if self._lite:
            logger.warn("XferSetupRaw.offloadable_extended() is undefined in 'lite' mode.")
            return []
        else:
            return sorted(self._offloadable_difference, key=lambda x: x['source']['name'])


    @property
    def deletable(self):
        """ Return 'source/target names' deletable difference """
        if self._lite:
            logger.warn("XferSetupRaw.deletable() is undefined in 'lite' mode.")
            return []
        else:
            return sorted([(_['source']['name'], _['target']['name']) for _ in self._deletable_difference],
                          key=lambda x: x[0])

    @property
    def deletable_target(self):
        """ Return 'target files' deletable difference:
            Files that exist on target, but not on source
        """
        if self._lite:
            logger.warn("XferSetupRaw.deletable_target() is undefined in 'lite' mode.")
            return []
        else:
            return sorted([_['target'] for _ in self._deletable_difference])

    @property
    def deletable_files(self):
        """ Return 'target files' deletable difference:
            Files that exist on target, but not on source
        """
        if self._lite:
            logger.warn("XferSetupRaw.deletable_files() is undefined in 'lite' mode.")
            return []
        else:
            return sorted([_['target']['name'] for _ in self._deletable_difference])

    @property
    def deletable_keys(self):
        """ Return 'target keys' deletable difference:
            Files that exist on target, but not on source
        """
        if self._lite:
            logger.warn("XferSetupRaw.deletable_keys() is undefined in 'lite' mode.")
            return []
        else:
            return sorted([_['target']['key_name'] for _ in self._deletable_difference])

    @property
    def deletable_distcp(self):
        """ Return deletable difference suitable to 'file tree' distcp copies

            Specifically, a list of:
            ([source key1, source key2, ...], target) tuples
        """
        if self._lite:
            logger.warn("XferSetupRaw.deletable_distcp() is undefined in 'lite' mode.")
            return []
        else:
            return self._prepare_distcp_difference(self._deletable_difference)

    @property
    def deletable_extended(self):
        """ Return all collected deletable difference (names, sizes etc) """
        if self._lite:
            logger.warn("XferSetupRaw.deletable_extended() is undefined in 'lite' mode.")
            return []
        else:
            return sorted(self._deletable_difference, key=lambda x: x['target']['name'])

    @property
    def search_conditions(self):
        """ Return search conditions """
        return self._search_conditions if self._search_conditions is not None else []

    @property
    def source_stats(self):
        """ Return source 'statistics' """
        return self._source_stats

    @property
    def target_stats(self):
        """ Return target 'statistics' """
        return self._target_stats

    @property
    def offloadable_stats(self):
        """ Return 'offloadable difference' 'statistics' """
        return self._offloadable_stats

    @property
    def deletable_stats(self):
        """ Return 'deletable difference' 'statistics' """
        return self._deletable_stats

    @property
    def in_sync(self):
        """ Return True if SRC == DST
        """
        if self._lite:
            logger.warn("XferSetupRaw.in_sync() is undefined in 'lite' mode.")
            return False
        else:
            return not (self.offloadable or self.deletable)

    ###########################################################################
    # PUBLIC ROUTINES
    ###########################################################################

    def evaluate(self, search_conditions=None, lite=False):
        """ Evaluate source/destination pair for transfer

            :search_conditions: JMESpath search conditions
            :lite: Special "lite" setup
                Assumes that we need to access "source" files quickly and do not care about target.

                In "lite" mode:

                1. Configuration options: .source, .target, .._drv, ..url are available
                2. Data options: Only .source_all is available (in a streaming fashion)
        """

        logger.debug("Evaluating for offload. Search conditions: %s. Lite mode: %s" % (search_conditions, lite))

        # Calculate offloadable/deletable difference
        self._evaluate(search_conditions, lite)

    def prepare_execution(self):
        """ Prepare storage engines for execution
            i.e. create 'root' directories on target
        """

        logger.debug("Preparing 'execution' for target")
        if not self._target.exists:
            self._target.create()


###############################################################################
# END CLASS: XferSetupRaw
###############################################################################

###############################################################################
# CLASS: XferSetup
###############################################################################
class XferSetup(XferSetupRaw):
    """ XferSetup: Prepare transfer

        between 'source' and 'target' configuration sections
        for a specific source/target table pair

           - Query contents
           - Calculate statistics (size, # of files, last_modified etc)
           - Calculate 'offloadable' difference
    """

    def __init__(self, source, target, source_db_table, target_db_table=None, cfg=None, lite=False):
        """ CONSTRUCTOR

        """
        assert source and target and source_db_table and (not cfg or isinstance(cfg, GOERemoteConfig))

        # Configuration object
        self._cfg = cfg or GOERemoteConfig()

        # Source/target sections, their types, components etc
        self._source_section_name = source
        self._target_section_name = target
        self._source_db_table = source_db_table
        self._target_db_table = target_db_table or source_db_table

        # Objects being worked on
        self._source_section = self._parse_section(source, 'source', source_db_table)
        self._target_section = self._parse_section(target, 'target', target_db_table)

        source_obj = self._init_object(source, self._source_section, source_db_table)
        target_obj = self._init_object(target, self._target_section, target_db_table)

        XferSetupRaw.__init__(self, source_obj, target_obj, lite=lite)

        logger.debug("XferSetup() object initialized for source: %s:%s, destination: %s:%s" % \
            (source, source_db_table, target, target_db_table))

    ###########################################################################
    # PRIVATE ROUTINES
    ###########################################################################

    def _parse_section(self, section, destination, db_table):
        """ Parse configuration 'section' into:
            {
                'name': section,
                'destination': destination,
                'type': s3, hdfs...,
                'db_path_suffix': ...,
                'base_url': url adjusted for 'distcp', based on type,
                s3: {'bucket': ..., 'prefix': ...}
                hdfs: {'webhdfs_url': ..., 'root': ...}
            }
        """
        assert self._cfg

        if not self._cfg.has_section(section):
            raise OffloadLogicException("Section: '%s' does NOT exist in configuration file" % section)
        if not self._cfg.has_option(section, 'db_path_suffix'):
            raise OffloadLogicException("'db_path_suffix' is missing in section: '%s'" % section)

        result = {
            'name': section,
            'destination': destination,
            'type': self._determine_section_type(section, destination),
            'db_path_suffix': self._cfg.get(section, 'db_path_suffix'),
        }

        if DST_HDFS == result['type']:
            result.update(self._parse_section_hdfs(section))
        elif DST_S3 == result['type']:
            result.update(self._parse_section_s3(section))
        else:
            raise OffloadLogicException("Unsupported section type: %s for '%s'" % \
                (result['type'], destination))

        result['db_table_url'] = self._cfg.table_url(section, db_table)
        result['db_url'] = result['base_url'] + result['db_table_url']

        logger.debug("Parsed '%s' section: %s" % (destination, result))
        return result

    def _determine_section_type(self, section, destination):
        """ Determine section type and make sure it's valid
        """
        assert self._cfg

        # First, query 'type' keyword directly
        config_type = self._cfg.get(section, 'type') \
            if self._cfg.has_option(section, 'type') else None

        if config_type and config_type in (DST_HDFS, DST_S3):
            section_type = config_type
        elif self._cfg.has_option(section, 's3_bucket'):
            section_type = DST_S3
        elif self._cfg.has_option(section, 'hdfs_root'):
            section_type = DST_HDFS
        else:
            raise OffloadLogicException("Unable to determine section type for '%s'" % section)

        logger.debug("Determined '%s' type to be: %s" % (destination, section_type))
        return section_type

    def _parse_section_hdfs(self, section):
        """ Validates HDFS section and parses it into: webhdfs_url, root, base_url

            i.e.:

            [<section>]

            hdfs_root: /user/goe/offload
            hdfs_url: hdfs://localhost:8020
            webhdfs_url: http://localhost:50070

            into:
            {
                webhdfs_url: http://localhost:50070,
                root: /user/goe/offload,
                base_url: hdfs://localhost:8020/user/goe/offload
            }
        """
        assert self._cfg

        # Validate HDFS section components
        for component in ('hdfs_root', 'hdfs_url', 'webhdfs_url'):
            if not self._cfg.has_option(section, component):
                raise OffloadLogicException("Required components: '%s' is missing in HDFS section: %s" % \
                    (component, section))

        hdfs_root = self._cfg.get(section, 'hdfs_root')
        if not hdfs_root.startswith('/'):
            hdfs_root = '/' + hdfs_root
        if not hdfs_root.endswith('/'):
            hdfs_root += '/'

        hdfs_url = self._cfg.get(section, 'hdfs_url')
        if hdfs_url.endswith('/'):
            hdfs_url = hdfs_url[:-1]

        result = {
            'webhdfs_url': self._cfg.get(section, 'webhdfs_url'),
            'root': hdfs_root,
            'base_url': hdfs_url + hdfs_root
        }

        logger.debug("Parsed HDFS section: %s into: %s" % (section, result))
        return result

    def _parse_section_s3(self, section):
        """ Validates S3 section and parses it into: bucket, prefix, base_url

            i.e.:

            [<section>]

            s3_bucket: goe.backup
            s3_prefix: user/goe/offload

            into:
            {
                bucket: goe.backup
                prefix: user/goe/offload
                base_url: s3n://goe.backup/user/goe/offload
            }
        """
        assert self._cfg

        # Validate HDFS section components
        for component in ('s3_bucket', 's3_prefix'):
            if not self._cfg.has_option(section, component):
                raise OffloadLogicException("Required components: '%s' is missing in S3 section: %s" % \
                    (component, section))

        bucket = self._cfg.get(section, 's3_bucket')

        prefix = self._cfg.get(section, 's3_prefix')
        if prefix.startswith('/'):
            prefix = prefix[1:]
        if not prefix.endswith('/'):
            prefix += '/'

        result = {
            'bucket': bucket,
            'prefix': prefix,
            'base_url': 's3n://' + bucket + '/' + prefix,
        }

        logger.debug("Parsed S3 section: %s into: %s" % (section, result))
        return result

    def _init_object(self, section_name, section_object, db_table):
        """ Initialize object for specific table based on 'type'
        """
        obj_type = section_object['type']

        if DST_HDFS == obj_type:
            return self._init_object_hdfs(section_object['db_table_url'], section_name)
        elif DST_S3 == obj_type:
            return self._init_object_s3(section_object['db_table_url'], section_name)
        else:
            raise OffloadLogicException("Unrecognized object type: '%s'" % obj_type)

    def _init_object_hdfs(self, root, section_name):
        """ Initialize HdfsStore object for a specific root
        """
        return HdfsStore(self._cfg, section_name, root)

    def _init_object_s3(self, prefix, section_name):
        """ Initialize S3Store object for a specific bucket/prefix
        """
        return S3Store(self._cfg, section_name, prefix)

    ###########################################################################
    # PROPERTIES
    ###########################################################################

    @property
    def source(self):
        """ Return 'source'
        """
        return self._source_section_name

    @property
    def source_db_table(self):
        """ Return source 'db.table'
        """
        return self._source_db_table

    @property
    def source_url(self):
        """ Return 'source' db_url
        """
        return self._source_section['db_url']

    @property
    def source_db_table_url(self):
        """ Return 'source' 'db_table' url
        """
        return self._source_section['db_table_url']

    @property
    def source_type(self):
        """ Return source 'type' """
        return self._source_section['type']

    @property
    def target(self):
        """ Return 'target'
        """
        return self._target_section_name

    @property
    def target_db_table(self):
        """ Return target 'db.table'
        """
        return self._target_db_table

    @property
    def target_url(self):
        """ Return 'target' db_url
        """
        return self._target_section['db_url']

    @property
    def target_db_table_url(self):
        """ Return 'target' 'db_table' url
        """
        return self._target_section['db_table_url']

    @property
    def target_type(self):
        """ Return target 'type' """
        return self._target_section['type']


###############################################################################
# END CLASS: XferSetup
###############################################################################

if __name__ == "__main__":
    import sys
    import inspect

    from goe.util.misc_functions import set_goelib_logging, options_list_to_namespace

    def usage(prog_name):
        print("%s: property src src_db_table dst dst_db_table [lite=True|False] [debug level]" % prog_name)
        sys.exit(1)


    def main():
        prog_name = sys.argv[0]

        if len(sys.argv) < 6:
            usage(prog_name)

        log_level = sys.argv[-1:][0].upper()
        if log_level not in ('DEBUG', 'INFO', 'WARNING', 'CRITICAL', 'ERROR'):
            log_level = 'CRITICAL'
        set_goelib_logging(log_level)

        prop, source, source_db_table, destination, destination_db_table = sys.argv[1:6]
        args = [_ for _ in sys.argv[6:] if _.upper() != log_level]
        user_options=vars(options_list_to_namespace(args))
        lite=user_options.get('lite', False)

        setup = XferSetup(source, destination, source_db_table, destination_db_table, GOERemoteConfig())
        setup.evaluate(lite=lite)

        obj = getattr(setup, prop)
        if isinstance(obj, dict):
            for k, v in list(obj.items()):
                print("%s: %s" % (k, v))
        elif isinstance(obj, list):
            for _ in obj:
                print(_)
        elif inspect.isgenerator(obj):
            for _ in obj:
                print(_)
        else:
            print(str(obj))


    main()
