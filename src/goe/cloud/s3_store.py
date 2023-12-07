#! /usr/bin/env python3
""" S3Store: Gluent s3/hive 'file tree' abstractions

    Represents 's3 bucket:prefix' as a 'hive table' file list
    with some limited query capabilities

    LICENSE_TEXT
"""

import logging
import re

import boto3
import botocore

from goe.cloud.gluent_layout import GluentHdfsLayout

from goe.util.jmespath_search import JMESPathSearch
from goe.util.misc_functions import end_by, chunk_list


###############################################################################
# EXCEPTIONS
###############################################################################
class S3StoreException(Exception): pass


###############################################################################
# CONSTANTS
###############################################################################

# Folder with 'meta' information (located under 'prefix')
# Contents is ignored when extracting file list
METANOTE_FOLDER='meta'

# Max S3 batch size (for batch deletes)
MAX_S3_DELETE_CHUNK = 1000


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler()) # Disabling logging by default


class S3Store(GluentHdfsLayout, JMESPathSearch, object):
    """ S3Store: Generic S3 store abstraction:
            File tree (flattened to {'full_name': {<file properties>}}
            starting from specific 'bucket:prefix'
    """

    def __init__(self, cfg, section, prefix='/'):
        """ CONSTRUCTOR
        """

        # Globally accessible configuration object
        # (config file pointed by $REMOTE_OFFLOAD_CONF)
        self._cfg = cfg
        # Relevant config file section
        self._cfg_section = section

        options = self._cfg.extract_options(self._cfg_section, ('s3_bucket', 's3_prefix'))
        root_prefix = end_by(options['s3_prefix'], '/').lstrip('/')
        self._prefix = root_prefix + prefix.lstrip('/')           # S3 key prefix  (IN)
        self._bucket = options['s3_bucket']                       # S3 bucket      (IN)

        # S3 boto objects
        self._s3 = S3Store.s3_resource()                    # Resource
        self._s3_client = S3Store.s3_client()               # Low level client
        self._s3_bucket = self._s3.Bucket(self._bucket)     # Bucket

        self._data   = []                                         # Result data    (OUT)

        # S3Store initialization - collect RAW data from S3 bucket
        if not self._bucket_exists():
            raise S3StoreException("S3 bucket: %s cannot be found" % self._bucket)

        logger.debug("S3Store() object successfully initialized for bucket: '%s', prefix: '%s'" % \
            (self._bucket, self._prefix))


    def __str__(self):
        return "[%s]: s3://%s/%s" % (self._cfg_section, self._bucket, self._prefix)

    def __repr__(self):
        return "[section={section}]: s3://{bucket}/{prefix}".format(
            section=self._cfg_section,
            bucket=self._bucket,
            prefix=self._prefix
        )


    ###########################################################################
    # STATIC ROUTINES
    ###########################################################################

    @staticmethod
    def s3_client():
        """ Low level s3 client """
        session = boto3.session.Session()
        return session.client('s3')


    @staticmethod
    def s3_resource():
        """ Low level s3 client """
        session = boto3.session.Session()
        return session.resource('s3')


    ###########################################################################
    # PRIVATE ROUTINES
    ###########################################################################

    def _bucket_exists(self):
        """ Return True if bucket exists, False otherwise
        """
        assert self._bucket
        exists = True

        try:
            self._s3_client.head_bucket(Bucket=self._bucket)
            logger.debug("Bucket: %s exists" % self._bucket)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                logger.warn("Bucket: %s does NOT exist" % self._bucket)
                exists = False
            else:
                raise

        return exists


    def _prefix_exists(self):
        """ Return True if bucket/prefix exists, False otherwise
        """
        assert self._bucket and self._prefix

        # Create a simple paginator, based on bucket:prefix that returns 1 record and exits
        # or 0 records if bucket:prefix does not exist
        items = [_ for _ in self._get_key_iterator(config={'MaxItems': 1, 'PageSize': 1})]
        exists = True if items else False

        logger.debug("Prefix: s3://%s/%s exists: %s" % (self._bucket, self._prefix, exists))
        return exists


    def _get_page_iterator(self, config=None):
        """ GENERATOR: Get 'object list' paginator for S3 self._bucket
            starting from self._prefix (a.k.a. 'root directory')

            Pagination is done in 'pages' that need to later be parsed into items
        """
        logger.debug("Getting keys for S3 bucket: %s starting from prefix: %s" % (self._bucket, self._prefix))

        # Check for bucket existence
        if self._bucket_exists():
            paginator = self._s3_client.get_paginator('list_objects')
            args = {
                'Bucket': self._bucket,
                'Prefix': self._prefix
            }
            if config:
                args['PaginationConfig'] = config
            return paginator.paginate(**args)
        else:
            logger.warn("S3 bucket: %s does NOT exist" % self._bucket)
            return []


    def _page_to_keys(self, page_iterator):
        """ GENERATOR: S3 page to item parser
        """
        for page in page_iterator:
            # Even if there is no 'Contents', page paginator still provides response, but w/o 'Contents' key
            if 'Contents' not in page:
                return
            for item in page['Contents']:
                yield item


    def _select_files_only(self, keys):
        """ GENERATOR: Filter out 'directories' in the list of 'keys'

            'directory' keys are named as ...$folder$
        """
        reD = re.compile('^.*\$folder\$$')            # Special 'folder' key that distcp makes
        reMeta = re.compile('^%s%s/' % (end_by(self._prefix, '/'), METANOTE_FOLDER)) # Meta folder (ignored)

        for key in keys:
            key_name = key['Key']
            if not reD.match(key_name) and not reMeta.match(key_name):
                yield key


    def _get_key_properties(self, key):
        """ Get key 'properties': size, md5, last_modified for a specified key
        """
        return {
            'size': key['Size'],
            # Using it only for display now, so convert to isoformat
            'last_modified': key['LastModified'].isoformat().encode('ascii'),

            # TODO: maxym@ 2015-12-31
            # Ignoring checksum for now as I cannot find a way to properly compare it with HDFS checksum
            # TODO: maxym@ 2016-12-16
            # It is possible to extract 'HDFS checksum' from 'checksum' Metadata key
            # However, this requires 1 extra head_object() call per key as s3 'object paginator' does not return it
            # ignoring for now
            #'checksum': key['Etag'][1:-1],
        }


    def _get_file_data(self, keys):
        """ GENERATOR: Emit 'file data' for each 'key'
        
            Emits: {'key': ..., 'size': ..., 'timestamp': ..., 'last_modified': ...},
        """
        for key in keys:
            file_properties = self._get_key_properties(key)
            file_key = key['Key'].replace(end_by(self._prefix, '/'), '')
            file_properties['key_name'] = file_key

            # Add gluent timestamp if possible
            file_properties = self.add_gluentts(file_properties, file_key)
            # Mine for 'partitions'
            file_properties = self.add_partitions(file_properties, file_key)
            
            logger.debug("Key: %s properties: %s" % (file_key, file_properties))
            yield file_properties
    

    def _get_contents(self, stream=True):
        """ Get list of keys for self._bucket/self._prefix
            and their metadata (size, md5, last_modified)
        """
        bucket_pages = self._get_page_iterator()
        bucket_keys = self._page_to_keys(bucket_pages)
        bucket_files = self._select_files_only(bucket_keys)

        if stream:
            return self._get_file_data(bucket_files)
        else:
            return [_ for _ in self._get_file_data(bucket_files)]


    ###########################################################################
    # PROPERTIES
    ###########################################################################

    @property
    def bucket(self):
        return self._bucket


    @property
    def prefix(self):
        return self._prefix


    @property
    def exists(self):
        return self._bucket_exists() and self._prefix_exists()


    @property
    def data(self):
        """ Get RAW S3 data (complete list) """
        self._data = self._get_contents(stream=False)
        return self._data


    @property
    def stream(self):
        """ Get RAW S3 data (stream) """
        return self._get_contents(stream=True)


    @property
    def section(self):
        """ Get cfg file section used when the object was initialized """
        return self._cfg_section


    ###########################################################################
    # PUBLIC ROUTINES
    ###########################################################################

    def create(self):
        """ Create S3 prefix """
        logger.debug("S3 does not need to pre-create 'prefix', it will be done 'automatically'")


    def save_meta_note(self, file_name, data, overwrite=True, stats=None):
        """ Write 'data' to a simple file: metafolder/file_name """

        # TODO: maxym@ 2016-10-03 - Implement 'no overwrite' semantics

        key_name = "%s%s/%s" % (end_by(self._prefix, '/'), METANOTE_FOLDER, file_name)

        logger.debug('Saving S3 note: %s:%s' % (self._bucket, key_name))
        self._s3_bucket.put_object(Key=key_name, Body=data)
        self._s3_bucket.wait_until_exists()


    def read_meta_note(self, file_name):
        """ Read simple file: metafolder/file_name and return data """
        key_name = "%s%s/%s" % (end_by(self._prefix, '/'), METANOTE_FOLDER, file_name)

        logger.debug('Reading S3 note: %s:%s' % (self._bucket, key_name))

        s3_object = self._s3.Object(self._bucket, key_name)
        
        return s3_object.get()['Body'].read()


    def delete(self, file_list):
        """ Delete files from under _prefix """

        for record_range in chunk_list(file_list, MAX_S3_DELETE_CHUNK):
            delete_objects = {
                'Objects': [ {'Key': end_by(self._prefix, '/') + _} for _ in record_range]
            }
        
            # S3 batch delete
            logger.info("Delete S3 batch: %s" % delete_objects)
            self._s3_bucket.delete_objects(Delete=delete_objects)


if __name__ == "__main__":
    import sys

    from goe.util.misc_functions import set_gluentlib_logging
    from goe.util.config_file import GluentRemoteConfig

    def usage(prog_name):
        print("%s: section prefix [debug level]" % prog_name)
        sys.exit(1)


    def main():
        prog_name = sys.argv[0]

        if len(sys.argv) < 3:
            usage(prog_name)

        section, prefix = sys.argv[1:3]

        log_level = sys.argv[3].upper() if len(sys.argv) > 3 else 'CRITICAL'
        set_gluentlib_logging(log_level)

        cfg = GluentRemoteConfig()

        s3 = S3Store(cfg, section, prefix)
        print("Using client: %s" % repr(s3))

        for _ in s3.stream:
            print(_)


    main()


