#! /usr/bin/env python3
""" HdfsStore: GOE HDFS/hive 'file tree' abstractions

    Represents 'HDFS root' as a 'hive table' file list
    with some limited query capabilities

    LICENSE_TEXT
"""

import datetime
import logging
import os.path
import pytz
import re

from goe.cloud.goe_layout import GOEHdfsLayout

from goe.filesystem.cli_hdfs import CliHdfs
from goe.filesystem.goe_dfs import WebHdfs, GOEWebHdfsClient
from goe.util.jmespath_search import JMESPathSearch
from goe.util.misc_functions import end_by, parse_python_from_string


###############################################################################
# EXCEPTIONS
###############################################################################
class HdfsStoreException(Exception): pass


###############################################################################
# CONSTANTS
###############################################################################

# Folder with 'meta' information (located under 'root')
# Contents is ignored when extracting file list
METANOTE_FOLDER='meta'

# HDFS file "prefix" with host:port
RE_HDFS_PREFIX = re.compile("^(hdfs|http|https)://(\S+):(\d+)(.*)$", re.I)

# Default 'new directory' permissions (when 'root' is created)
DEFAULT_ROOT_PERMISSIONS=750

# HDFS client types
HDFS_TYPE_SSH="<ssh>"
HDFS_TYPE_WEBHDFS="<webhdfs>"


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler()) # Disabling logging by default


###############################################################################
# CLASS: HdfsStore
###############################################################################
class HdfsStore(GOEHdfsLayout, JMESPathSearch, object):
    """ HdfsStore: Generic HDFS store abstraction:
            File tree (flattened to {'full_name': {<file properties>}}
            starting from specific 'root'
    """

    def __init__(self, cfg, section, root, dry_run=False):
        """ CONSTRUCTOR
        """
        # Globally accessible configuration object
        # (config file, usually pointed by $REMOTE_OFFLOAD_CONF)
        self._cfg = cfg
        # Relevant config file section
        self._cfg_section = section

        # Extract user from configuration file
        if not self._cfg_section:
            raise HdfsStoreException("Configuration 'section' was not supplied to HdfsStore() object")

        # HDFS object - supports different 'connection classes'
        # with class: argument in config section and appropriate parameter list
        # See: http://hdfscli.readthedocs.org/en/latest/quickstart.html#configuration
        self._hdfs = HdfsStore.hdfscli_connect(self._cfg, self._cfg_section, dry_run)
        if not isinstance(self._hdfs, GOEWebHdfsClient):
            raise HdfsStoreException("Only webhdfs client is supported in HdfsStore so far")

        options = self._cfg.extract_options(self._cfg_section, ('webhdfs_url', 'hdfs_root'))
        root_prefix = end_by(options['hdfs_root'], '/')
        self._root = root_prefix + root.lstrip('/')   # HDFS root  (IN)

        self._data = []         # HDFS (raw) contents (OUT)

        # HdfsStore initialization - collect RAW data from HDFS
        if not self._root_exists():
            logger.warn("HDFS root: %s cannot be found." % root)

        logger.debug("HdfsStore() object successfully initialized for root: '%s'" % self._root)


    def __str__(self):
        return "[%s]: hdfs://%s" % (self._cfg_section, self._root)

    def __repr__(self):
        return "[section={section} client={client} user={user} url={url}]: hdfs://{root}".format(
            section=self._cfg_section,
            client=self._hdfs,
            user=self._cfg.get(self._cfg_section, 'hdfs_user', '<current user>'),
            url=self._cfg.get(self._cfg_section, 'webhdfs_url', '<unknown>'),
            root=self._root
        )


    ###########################################################################
    # STATIC ROUTINES
    ###########################################################################

    @staticmethod
    def hdfscli_goe_client(cfg, section, dry_run=False):
        """ Return 'high level' WebHdfs or CliHdfs HDFS client
        """
        def web_client(webhdfs_url):
            """ Return WebHdfs Client
            """
            logger.debug("hdfscli_goe_client: Instantiating WebHdfs client")

            _, host, port, _ = hdfs_split(webhdfs_url)
            if not host or port is None:
                raise HdfsStoreException("Invalid 'webhdfs_url': %s" % url)

            args = {
                'hdfs_namenode': host,
                'webhdfs_port': int(port),
                'hdfs_user': cfg.get(section, 'hdfs_user'),
                'use_kerberos': parse_python_from_string(cfg.get(section, 'hdfs_use_kerberos', "False")),
                'verify_ssl_cert': parse_python_from_string(cfg.get(section, 'hdfs_verify_ssl_cert', "None")),
                'mutual_auth': cfg.get(section, 'hdfs_mutual_auth', 'OPTIONAL'),
                'max_concurrency': cfg.getint(section, 'hdfs_request_max_concurrency', 1),
                'dry_run': dry_run
            }

            return WebHdfs(**args)

        def ssh_client(user):
            """ Return CliHdfs Client
            """
            logger.debug("hdfscli_goe_client: Instantiating CliHdfs client")

            hdfs_host = cfg.get(section, 'hdfs_cmd_host') or cfg.get(section, 'hadoop_host')
            if not user or not hdfs_host:
                raise HdfsStoreException("Invalid setup for HDFS ssh section. Define: 'hadoop_host', 'ssh_user'")

            return CliHdfs(
                hdfs_host = hdfs_host,
                ssh_user = user,
                tmp_dir = cfg.get(section, 'hdfs_tmp_dir', '/tmp'),
                dry_run = dry_run
            )

        # connect() begins here
        assert cfg and section

        url = cfg.get(section, 'webhdfs_url')
        ssh_user = cfg.get(section, 'ssh_user')
        if url:
            return web_client(url)
        elif ssh_user:
            return ssh_client(ssh_user)
        else:
            raise HdfsStoreException(
                "Unable to create HDFS client from section: %s. Supply either: 'webhdfs_url' or 'ssh_user'" % section
            )


    @staticmethod
    def hdfscli_connect(cfg, section, dry_run=False):
        """ Return 'raw' hdfscli client (from 'hdfs' library)
        """
        return HdfsStore.hdfscli_goe_client(cfg, section, dry_run).client


    ###########################################################################
    # PRIVATE ROUTINES
    ###########################################################################

    def _root_exists(self):
        """ Returns: True if self._root exists, False otherwise
        """
        assert self._root

        if self._hdfs.status(self._root, strict=False):
            logger.debug("HDFS (root) path: %s exists" % self._root)
            return True
        else:
            logger.debug("HDFS (root) path: %s does NOT exist" % self._root)
            return False


    def _create_root(self):
        """ Create HDFS self._root directory
        """
        assert self._root

        if not self._cfg_section:
            raise HdfsUserException("Configuration 'section' needs to be supplied to 'create root': %s" % \
                self._root)

        permissions = self._cfg.getint(self._cfg_section, 'hdfs_newdir_permissions', DEFAULT_ROOT_PERMISSIONS)
        if not permissions:
            raise HdfsUserException("('hdfs_newdir_permissions') "
                                    "need to be defined in '%s' section to create root: %s" % \
                                    (self._cfg_section, self._root))

        try:
            logger.debug("Creating HDFS root: %s with permissions: %s" % (self._root, permissions))
            self._hdfs.makedirs(self._root, permissions)
        except hdfs.util.HdfsError as e:
            raise HdfsStoreException("HDFS exception: %s while creating root: %s" % (e, self._root))


    def _extract_files(self):
        """ GENERATOR: Extract and emit files + their "status properties"
            recursively from HDFS self._root location (a.k.a. 'root directory')

            Emit (file, status), Skip directories

            Returns [] if 'root' does not exist
        """
        logger.debug("Extracting HDFS contents, starting from root: %s" % self._root)
        skip_meta = end_by(self._root, '/') + METANOTE_FOLDER

        for root, dirs, files in self._hdfs.walk(hdfs_path=self._root, depth=0, status=True):
            if skip_meta == root[0]:
                continue
            for f in files:
                file_name, file_status = f
                # 'root' is a tuple: ('file name', 'status'). We only need the 1st part
                abs_file_name = os.path.join(root[0], file_name)
                logger.debug("Emitting file: %s with status: %s" % (abs_file_name, file_status))
                yield (abs_file_name, file_status)


    def _get_file_properties(self, file_name, file_status):
        """ Get 'properties': size, md5, last_modified for a specified file
        """
        last_modified = datetime.datetime.fromtimestamp(int(file_status['modificationTime']/1000), pytz.utc)

        return {
            'size': file_status['length'],

            # HDFS Last modification time is in 'milliseconds'
            # Also, assuming that the time is: 'UTC' for now
            # Using it only for display now, so convert to isoformat
            'last_modified': last_modified.isoformat().encode('ascii'),

            # Ignoring checksum for now as I cannot find a way to properly compare it with AWS checksum
            # maxym@ 2015-12-31
            #'checksum': self._hdfs.checksum(file_name)['bytes'],
        }


    def _make_keys(self, file_tuples):
        """ GENERATOR: For each file, emit file metadata records

            {'key': ..., 'size': ..., 'timestamp': ..., 'last_modified': ...},
        """

        for file_name, file_status in file_tuples:
            file_properties = self._get_file_properties(file_name, file_status)
            file_key = file_name.replace(end_by(self._root, '/'), '')
            file_properties['key_name'] = file_key

            # Add GOE timestamp if possible
            file_properties = self.add_goets(file_properties, file_key)
            # Mine for 'partitions'
            file_properties = self.add_partitions(file_properties, file_key)

            logger.debug("Key: %s properties: %s" % (file_key, file_properties))
            yield file_properties


    def _get_contents(self, stream=True):
        """ Get list of files for self._root HDFS prefix
            and their metadata (size, md5, last_modified)
        """
        if not self._root_exists():
            logger.warn("HDFS root: %s does NOT exist. Nothing to return" % self._root)
            return []

        hdfs_file_tuples = self._extract_files()
        file_keys = self._make_keys(hdfs_file_tuples)

        if stream:
            return file_keys
        else:
            return [_ for _ in file_keys]


    ###########################################################################
    # PROPERTIES
    ###########################################################################

    @property
    def client(self):
        return self._hdfs


    @property
    def root(self):
        return self._root


    @property
    def exists(self):
        return self._root_exists()


    @property
    def data(self):
        """ Get RAW HDFS data (complete list) """
        self._data = self._get_contents(stream=False)
        return self._data


    @property
    def stream(self):
        """ Get RAW HDFS data (stream) """
        return self._get_contents(stream=True)


    @property
    def section(self):
        """ Get cfg file section used when the object was initialized """
        return self._cfg_section


    ###########################################################################
    # PUBLIC ROUTINES
    ###########################################################################

    def create(self):
        """ Create HDFS root directory """
        if self._root_exists():
            logger.warning("Ignoring attempt to create already existing root: %s" % self._root)
        else:
            self._create_root()


    def save_meta_note(self, file_name, data, overwrite=True, stats=None):
        """ Write 'data' to a simple file: metafolder/file_name """

        key_name = "%s%s/%s" % (end_by(self._root, '/'), METANOTE_FOLDER, file_name)

        logger.debug('Saving HDFS note: %s. Overwrite: %s. With stats: %s' % (key_name, overwrite, stats))
        args = {
            'hdfs_path': key_name,
            'data': data,
            'overwrite': overwrite
        }

        if stats:
            for stat in ('permission', 'blocksize', 'replication', 'encoding'):
                if stat in stats:
                    args[stat] = stats[stat]

        self._hdfs.write(**args)


    def read_meta_note(self, file_name):
        """ Read simple file: metafolder/file_name and return data """
        key_name = "%s%s/%s" % (end_by(self._root, '/'), METANOTE_FOLDER, file_name)

        logger.debug('Reading HDFS note: %s' % key_name)

        with self._hdfs.read(key_name) as reader:
            data = reader.read()
            return data


    def delete(self, file_list):
        """ Delete files from under _root """

        delete_files = [end_by(self._root, '/') + _ for _ in file_list]

        for hdfs_file in delete_files:
            logger.info("Deleting HDFS file: %s" % hdfs_file)
            self._hdfs.delete(hdfs_file, recursive=False)


###########################################################################
# STANDALONE ROUTINES
###########################################################################

def hdfs_strip_host(hdfs_path):
    """ Strip host:port from HDFS path
        i.e. "hdfs://10.45.0.21:50070/myfile" -> /myfile
    """
    assert hdfs_path

    match = RE_HDFS_PREFIX.match(hdfs_path)
    if match:
        return match.group(4)
    else:
        return hdfs_path


def hdfs_split(hdfs_path):
    """ Split: hdfs://10.45.0.21:50070/myfile
        into: hdfs, 10.45.0.21, 50070, /myfile
    """
    assert hdfs_path

    match = RE_HDFS_PREFIX.match(hdfs_path)
    if match:
        return match.groups()
    else:
        return None, None, None, None


if __name__ == "__main__":
    import sys

    from goe.util.config_file import GOERemoteConfig
    from goe.util.misc_functions import set_goelib_logging

    def usage(prog_name):
        print("%s: config root [debug level]" % prog_name)
        sys.exit(1)


    def main():
        prog_name = sys.argv[0]

        if len(sys.argv) < 3:
            usage(prog_name)

        section, relative_root = sys.argv[1:3]

        log_level = sys.argv[3].upper() if len(sys.argv) > 3 else 'CRITICAL'
        set_goelib_logging(log_level)

        hdfs = HdfsStore(GOERemoteConfig(), section, relative_root)
        print("Using client: %s" % repr(hdfs))

        for _ in hdfs.stream:
            print(_)


    main()
