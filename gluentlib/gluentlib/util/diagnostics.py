# LICENSE_TEXT

import collections
import copy
import inspect
import json
import os
import re
import socket
import subprocess
import time
import traceback
import urllib.parse
import zipfile
from datetime import datetime, timedelta

from gluent import version
from gluentlib.config.orchestration_config import OrchestrationConfig
from gluentlib.offload.backend_api import BackendApiException
from gluentlib.offload.factory.backend_api_factory import backend_api_factory
from gluentlib.offload.factory.frontend_api_factory import frontend_api_factory
from gluentlib.offload.factory.offload_source_table_factory import OffloadSourceTable
from gluentlib.offload.offload_constants import DBTYPE_SPARK
from gluentlib.offload.offload_messages import SUPPRESS_STDOUT, VERBOSE, VVERBOSE
from gluentlib.offload.offload_source_table import HYBRID_ALL_OBJECTS
from gluentlib.offload.offload_transport_functions import ssh_cmd_prefix
from requests import Session
from requests.packages import urllib3
from urllib3.exceptions import InsecureRequestWarning

PermissionsTuple = collections.namedtuple('PermissionsTuple', 'type path recursive')

DIVIDER = '=' * 100
SUB_DIVIDER = '-' * 100

# Suppress warning from urllib3 from self-signed certificates.
urllib3.disable_warnings(category=InsecureRequestWarning)


class DiagnoseException(Exception):
    pass


class Diagnose(object):
    """ Class for gathering diagnostic information.
    """

    def __init__(self, target, verbose, messages):

        self._messages = messages

        self.GLUENT_DIAG_FILES_PREFIX = 'gluent_diag'
        self.GLUENT_DIAG_FILES_SUFFIX = '.txt'
        self.EXCLUDED_FILE_EXTENSIONS = ['.zip', '.tar', '.gz', '.bz2', self.GLUENT_DIAG_FILES_SUFFIX]

        self._orchestration_config = OrchestrationConfig.from_dict({'verbose': verbose})
        self._backend_api = backend_api_factory(target, self._orchestration_config, messages)
        self._frontend_api = frontend_api_factory(self._orchestration_config.db_type, self._orchestration_config,
                                                  messages, trace_action='Diagnose')
        self._backend_target = self._backend_api.backend_db_name

        if not self._frontend_api.gluent_diagnose_supported():
            raise DiagnoseException('Diagnose is not supported on system: {}'.format(
                self._frontend_api.frontend_db_name()))

        if self._orchestration_config.use_ssl:
            self._webui_protocol = 'https'
        else:
            self._webui_protocol = 'http'

    @staticmethod
    def from_options(options, messages=None):
        return Diagnose(target=options.target,
                        verbose=options.verbose,
                        messages=messages)

    @staticmethod
    def from_dict(diagnose_dict, messages=None):
        return Diagnose(target=diagnose_dict.get('target'),
                        verbose=diagnose_dict.get('verbose'),
                        messages=messages)

    def _log(self, msg, verbosity):
        """ Write msg at verbosity to messages.log if we initialised with a messages object
        """
        if self._messages:
            self._messages.log(msg, verbosity)

    def _debug(self, msg):
        """ Write msg to messages.debug if we initialised with a messages object
        """
        try:
            caller = inspect.stack()[1][3]
        except:
            caller = 'unknown'
        if self._messages:
            self._messages.debug('[DEBUG:%s] %s' % (caller, msg), detail=SUPPRESS_STDOUT)

    def _common_tuple_log_response(self, log_response):
        """ Common formatting for backend payloads described as a list of label/value tuples. """

        def line(text):
            return '{0}'.format(text)

        def header(header_character='='):
            return line(header_character * 100)

        def blank_line():
            return line('')

        resp = []
        for i, (label, payload) in enumerate(log_response):
            if i > 0:
                resp.append(blank_line())
            resp.append(header())
            resp.append(line(label))
            if payload:
                resp.append(header())
                resp.append(payload)
        return resp

    def _os_call(self, cmd):
        """ Run cmd and return results.
        """
        self._debug(cmd)
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = proc.communicate()
            if err or proc.returncode is None or proc.returncode > 0:
                if err == b'':
                    err = "OS call raised return code %s" % proc.returncode
                raise DiagnoseException('Error encountered running OS command: %s\n%s' % (cmd, err))
        except Exception as exc:
            raise DiagnoseException('Error encountered running OS command: %s\n%s' % (cmd, traceback.format_exc()))

        return out

    def _validate_directory(self, directory, check_read=True, check_write=True):
        """ Test directory exists and we can read and write it.
        """
        self._debug((directory, check_read, check_write))
        if not os.path.isdir(directory):
            raise DiagnoseException('%s is not a directory' % directory)
        if not os.access(directory, os.R_OK) and check_read:
            raise DiagnoseException('Unable to read %s directory' % directory)
        if not os.access(directory, os.W_OK) and check_write:
            raise DiagnoseException('Unable to write %s directory' % directory)

    def _validate_file(self, filename, check_read=True, check_write=True):
        """ Test filename exists and we can read and write it.
        """
        self._debug((filename, check_read, check_write))
        if not os.path.isfile(filename):
            raise DiagnoseException('%s is not a file' % filename)
        if not os.access(filename, os.R_OK) and check_read:
            raise DiagnoseException('Unable to read %s file' % filename)
        if not os.access(filename, os.W_OK) and check_write:
            raise DiagnoseException('Unable to write %s file' % filename)

    def _rm_file(self, filename):
        """ Remove filename.
        """
        self._debug(filename)
        try:
            os.remove(filename)
        except Exception as exc:
            raise DiagnoseException('Error encountered removing file: %s\n%s' % (filename, traceback.format_exc()))

    def _test_raw_conn(self, host, port, timeout=2):
        """ Test connection to host and port.
        """
        self._debug((host, port, timeout))
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)

        try:
            s.connect((host, int(port)))
            return True
        except Exception as exc:
            self._log('Error encountered in _test_raw_conn at %s:%s\n%s' % (host, port, traceback.format_exc()), VVERBOSE)
            return False
        finally:
            if s:
                s.close()

    def _get_url(self, url, return_type='text'):
        """ Get URL and return response.
        """
        assert return_type in ['text', 'json'], 'return_type must be "text" or "json"'
        self._debug((url, return_type))
        session = Session()
        resp = None
        try:
            response = session.get(url, verify=False)
            self._debug('response: %s' % response)
            if response.status_code == 200:
                self._log('Request at %s successful' % url, VERBOSE)
                if return_type == 'text':
                    self._debug('First 100 chars response.text: %s' % response.text[:100])
                    self._debug('Last 100 chars response.text: %s' % response.text[-100:])
                    resp = response.text
                elif return_type == 'json':
                    self._debug('response.json(): %s' % response.json())
                    resp = response.json()
        except Exception as exc:
            self._log('Error encountered in _get_url for %s\n%s' % (url, traceback.format_exc()), VVERBOSE)
            resp = None
        finally:
            if session:
                session.close()
        return resp

    def _normalise_prefix(self, prefix):
        """ Return normalised prefix.
        """
        self._debug(prefix)
        prefix = prefix or self.GLUENT_DIAG_FILES_PREFIX
        assert isinstance(prefix, str), 'prefix must be a string'
        self._debug(prefix)
        return prefix

    def _normalise_suffix(self, suffix):
        """ Return normalised suffix.
        """
        self._debug(suffix)
        suffix = suffix or self.GLUENT_DIAG_FILES_SUFFIX
        assert isinstance(suffix, str), 'suffix must be a string'
        self._debug(suffix)
        return suffix

    def _get_offload_source_table(self, owner, name):
        """ Return instance of OffloadSourceTable
        """
        self._debug((owner, name))
        return OffloadSourceTable.create(owner, name, self._orchestration_config, self._messages)

    def _update_css_path(self, content):
        """ Update locally referenced CSS path with remote CDN path
        """
        return content.replace('/www/bootstrap', 'https://maxcdn.bootstrapcdn.com/bootstrap/3.1.1')

    ###############
    # PUBLIC API
    ###############
    @property
    def backend_target(self):
        return self._backend_target

    def create_file(self, output_location, fname, content, prefix=None, suffix=None, header=None, gluent_header=True):
        """ Create file with contents in output_location.
        """
        assert isinstance(output_location, str), 'output_location must be a string'
        assert isinstance(fname, str), 'fname must be a string: %s' % type(fname)
        assert isinstance(content, list), 'content must be a list'
        prefix = self._normalise_prefix(prefix)
        suffix = self._normalise_suffix(suffix)
        self._validate_directory(output_location, check_read=False)

        filename = '%s_%s_%s_%s%s' % (prefix, fname, socket.getfqdn(), datetime.now().strftime('%Y-%m-%d_%H-%M-%S'), suffix)
        self._debug('filename: %s' % filename)
        try:
            with open(os.path.join(output_location, filename), 'wb') as out_file:
                if gluent_header:
                    out_file.write(b'\n%b' % DIVIDER.encode('utf-8'))
                    out_file.write(b'\nDiagnose v%b' % version().encode('utf-8'))
                    out_file.write(b'\nCopyright 2015-%b Gluent Inc. All rights reserved.'
                                   % datetime.now().strftime('%Y').encode('utf-8'))
                    out_file.write(b'\n%b' % DIVIDER.encode('utf-8'))
                    out_file.write(b'\n\nOutput file created at %b.\n\n'
                                   % datetime.now().strftime('%d %B %Y %H:%M:%S').encode('utf-8'))
                if header:
                    out_file.write(b'%b\n' % header.encode('utf-8'))
                for c in content:
                    out_file.write(b'%b\n' % c.encode('utf-8'))
        except Exception as exc:
            raise DiagnoseException('Unable to create file "%s"\n%s'
                                    % (os.path.join(output_location, filename), traceback.format_exc()))

        return os.path.join(output_location, filename)

    def remove_files(self, output_location, files, prefix=None, suffix=None):
        """ Remove files starting with prefix, ending with suffix from output_location.
        """
        assert isinstance(output_location, str), 'output_location must be a string'
        assert isinstance(files, list), 'files must be a list'
        prefix = self._normalise_prefix(prefix)
        suffix = self._normalise_suffix(suffix) if suffix else tuple(['.html', '.xml', self.GLUENT_DIAG_FILES_SUFFIX])
        self._validate_directory(output_location, check_read=False)
        rm_files = [f for f in files if f.startswith(os.path.join(output_location, prefix)) and f.endswith(suffix)]
        self._debug('rm_files: %s' % rm_files)
        for f in rm_files:
            self._validate_file(f)
            self._rm_file(f)

    def create_zip_archive(self, output_location, files, prefix=None):
        """ Create zip archive containing files in output_location
            Return the full zipfile path if successful, else None.
        """
        assert isinstance(output_location, str), 'output_location must be a string'
        assert isinstance(files, list), 'files must be a list'
        assert files, 'files must not be empty'
        prefix = self._normalise_prefix(prefix)
        self._debug((output_location, files, prefix))
        self._validate_directory(output_location, check_read=False)
        for f in files:
            self._validate_file(f, check_write=False)

        # de-duplicate the list: https://stackoverflow.com/questions/480214/how-do-you-remove-duplicates-from-a-list-whilst-preserving-order
        def dedupe(l):
            seen = set()
            seen_add = seen.add
            return [x for x in l if not (x in seen or seen_add(x))]

        files = dedupe(files)
        self._debug('len(files): %s' % len(files))
        zipfile_name = '%s_%s_%s.zip' % (prefix, socket.getfqdn(), datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))
        self._debug('zipfile_name: %s' % zipfile_name)
        zipfile_path = os.path.join(output_location, zipfile_name)
        self._debug('zipfile_path: %s' % zipfile_path)

        try:
            with zipfile.ZipFile(zipfile_path, 'w') as zip_fh:
                for f in files:
                    zip_fh.write(filename=f, arcname=os.path.basename(f), compress_type=zipfile.ZIP_DEFLATED)
            return zipfile_path
        except Exception as exc:
            raise DiagnoseException('Unable to create zip archive\n%s' % traceback.format_exc())

    def divide_list(self, lst, div=SUB_DIVIDER):
        """ Insert a divider after every entry in lst and return a new list
        """
        assert isinstance(lst, list), 'lst must be a list'
        assert isinstance(div, str), 'div must be a string'
        self._debug('len(lst): %s' % len(lst))
        self._debug('div: %s' % div)

        new_list = []
        for l in lst:
            new_list.extend([l])
            new_list.extend(['\n%s' % div])

        self._debug('len(new_list): %s' % len(new_list))
        return new_list

    def frontend_ddl(self, owner, name, object_type):
        """ Generate DDL for owner.name object of object_type.
        """
        assert isinstance(owner, str), 'owner must be a string'
        assert isinstance(name, str), 'name must be a string'
        assert isinstance(object_type, str), 'object_type must be a string'
        self._debug((owner, name, object_type))
        try:
            ddl = self._frontend_api.get_object_ddl(owner, name, object_type)
            self._debug(ddl)
        except Exception as exc:
            self._debug(str(exc))
            ddl = None
        return ddl

    def frontend_table_exists(self, db_name, table_name):
        return self._frontend_api.table_exists(db_name, table_name)

    def hybrid_schema_supported(self):
        return self._frontend_api.hybrid_schema_supported()

    def logs_last_n(self, log_location, n):
        """ Retrieve files in log_location with extensions not in self.EXCLUDED_FILE_EXTENSIONS list that have been
            modified in the last n hours.
        """
        assert re.search('[0-9]+(H|h|D|d)', n), 'Invalid value specified for n parameter. Valid examples include: 3h, 7d, 1H, 2D'
        assert isinstance(log_location, str), 'log_location must be a string'
        self._validate_directory(log_location)

        hrs = int(n[:-1])*24 if n[-1].upper() == 'D' else int(n[:-1])
        self._debug('hrs: %s' % str(hrs))
        r = []
        now = int(time.time())
        self._debug('now: %s' % str(now))
        for path, dnames, fnames in os.walk(log_location):
            self._debug('path: %s' % path)
            self._debug('dnames: %s' % dnames)
            self._debug('len(fnames): %s' % len(fnames))
            r.extend([os.path.join(path, x) for x in fnames if (now - int(os.path.getmtime(os.path.join(path, x))))/(60*60) < hrs and os.path.splitext(x)[1].lower() not in self.EXCLUDED_FILE_EXTENSIONS])

        self._debug('len(r): %s' % len(r))
        return r

    def logs_from(self, log_location, from_date_time='1970-01-01_00:00:00'):
        """ Retrieve files in log_location with extensions not in self.EXCLUDED_FILE_EXTENSIONS list that have been
            modified since from_date_time until the current date time.
        """
        assert datetime.strptime(from_date_time, '%Y-%m-%d_%H:%M:%S'), 'Invalid value specified for from_date_time parameter. Format must be: YYYY-MM-DD_HH24:MM:SS (e.g. 2018-06-19_15:23:05)'
        assert isinstance(log_location, str), 'log_location must be a string'
        self._validate_directory(log_location)

        from_dt = datetime.strptime(from_date_time, '%Y-%m-%d_%H:%M:%S')
        self._debug('from_dt: %s' % from_dt)
        r = []
        for path, dnames, fnames in os.walk(log_location):
            self._debug('path: %s' % path)
            self._debug('dnames: %s' % dnames)
            self._debug('len(fnames): %s' % len(fnames))
            r.extend([os.path.join(path, x) for x in fnames if from_dt <= datetime.fromtimestamp(os.path.getmtime(os.path.join(path, x))) and os.path.splitext(x)[1].lower() not in self.EXCLUDED_FILE_EXTENSIONS])

        self._debug('len(r): %s' % len(r))
        return r

    def logs_to(self, log_location, to_date_time=datetime.strftime(datetime.now(), '%Y-%m-%d_%H:%M:%S')):
        """ Retrieve files in log_location with extensions not in self.EXCLUDED_FILE_EXTENSIONS list that have been
            modified until to_date_time.
        """
        assert datetime.strptime(to_date_time, '%Y-%m-%d_%H:%M:%S'), 'Invalid value specified for to_date_time parameter. Format must be: YYYY-MM-DD_HH24:MM:SS (e.g. 2018-06-19_15:23:05)'
        assert isinstance(log_location, str), 'log_location must be a string'
        self._validate_directory(log_location)

        to_dt = datetime.strptime(to_date_time, '%Y-%m-%d_%H:%M:%S')
        self._debug('to_dt: %s' % to_dt)
        r = []
        for path, dnames, fnames in os.walk(log_location):
            self._debug('path: %s' % path)
            self._debug('dnames: %s' % dnames)
            self._debug('len(fnames): %s' % len(fnames))
            r.extend([os.path.join(path, x) for x in fnames if datetime.fromtimestamp(os.path.getmtime(os.path.join(path, x))) <= to_dt and os.path.splitext(x)[1].lower() not in self.EXCLUDED_FILE_EXTENSIONS])

        self._debug('len(r): %s' % len(r))
        return r

    def logs_between(self, log_location, from_date_time='1970-01-01_00:00:00', to_date_time=datetime.strftime(datetime.now(), '%Y-%m-%d_%H:%M:%S')):
        """ Retrieve files in log_location with extensions not in self.EXCLUDED_FILE_EXTENSIONS list that have been
            modified between from_date_time and to_date_time.
        """
        assert datetime.strptime(from_date_time, '%Y-%m-%d_%H:%M:%S'), 'Invalid value specified for from_date_time parameter. Format must be: YYYY-MM-DD_HH24:MM:SS (e.g. 2018-06-19_15:23:05)'
        assert datetime.strptime(to_date_time, '%Y-%m-%d_%H:%M:%S'), 'Invalid value specified for to_date_time parameter. Format must be: YYYY-MM-DD_HH24:MM:SS (e.g. 2018-06-19_15:23:05)'
        from_dt = datetime.strptime(from_date_time, '%Y-%m-%d_%H:%M:%S')
        to_dt = datetime.strptime(to_date_time, '%Y-%m-%d_%H:%M:%S')
        self._debug('from_dt: %s' % from_dt)
        self._debug('to_dt: %s' % to_dt)
        assert from_dt <= to_dt, 'Value specified for from_date_time parameter cannot be greater than value supplied for to_date_time parameter'
        assert isinstance(log_location, str), 'log_location must be a string'
        self._validate_directory(log_location)

        r = []
        for path, dnames, fnames in os.walk(log_location):
            self._debug('path: %s' % path)
            self._debug('dnames: %s' % dnames)
            self._debug('len(fnames): %s' % len(fnames))
            r.extend([os.path.join(path, x) for x in fnames if from_dt <= datetime.fromtimestamp(os.path.getmtime(os.path.join(path, x))) <= to_dt and os.path.splitext(x)[1].lower() not in self.EXCLUDED_FILE_EXTENSIONS])

        self._debug('len(r): %s' % len(r))
        return r

    def processes(self, grep_filter):
        """ List running processes matching grep_filter.
        """
        self._debug(grep_filter)
        out = self._os_call(['ps', '-eo', 'stat,pid,ppid,pgid,user,group,ruser,rgroup,start_time,time,cmd', '--cols=2000', '--sort=pgid,+ppid,+pid'])
        if out:
            output = ''
            for line in out.decode().split('\n'):
                self._debug(line)
                if any(gp in line for gp in grep_filter):
                    output += '%s\n' % line.strip()
            self._log('Processes matched: %s' % len([o for o in output.split('\n') if o]), VERBOSE)
            self._debug(output)
            return output
        else:
            self._log('Processes search returned no output', VERBOSE)
            return None

    def permissions(self, permissions_tuple):
        """ List permissions of tuple of the form (type, path, recursive).
        """
        assert isinstance(permissions_tuple, PermissionsTuple), 'permissions_tuple must be a PermissionsTuple'
        assert permissions_tuple.type in ('directory', 'file'), 'type must be one of "directory" or "file"'
        assert isinstance(permissions_tuple.path, str), 'path must be a string'
        assert permissions_tuple.recursive in [True, False], 'recursive must be "True" or "False"'
        self._debug(permissions_tuple)

        if permissions_tuple.type.lower() == 'directory':
            self._validate_directory(permissions_tuple.path, check_write=False)
        elif permissions_tuple.type.lower() == 'file':
            self._validate_file(permissions_tuple.path, check_write=False)

        # L flag dereferences symbolic links
        out = self._os_call(['ls', '-lL', '-%s' % ('R' if permissions_tuple.recursive else 'd'), permissions_tuple.path])
        if out:
            self._log('Permissions matched: %s' % len([o for o in out.decode().split('\n') if o]), VERBOSE)
            return out
        else:
            self._log('Permissions search returned no output', VERBOSE)
            return None

    def oracle_rewrite_definition(self, owner, name):
        """ Generate advanced rewrite definition.

            TODO@SS 9-Jul-18:

                This is not specific to an offloaded table so shouldn't be in OffloadSourceTable.
                However it really belongs in an as yet undefined class. Leaving this here for now
                for want of a better home.
        """
        assert isinstance(owner, str), 'owner must be a string'
        assert isinstance(name, str), 'name must be a string'
        self._debug((owner, name))

        params = {'owner': owner, 'name': name}
        q = """SELECT source_stmt
               ,      destination_stmt
               ,      rewrite_mode
               FROM   dba_rewrite_equivalences
               WHERE  owner = :owner
               AND    name = :name"""
        self._debug((q, params))
        source, destination, mode = self._frontend_api.execute_query_fetch_one(q, query_params=params,
                                                                               trace_action='diagnose_rewrite_definition')
        if source and destination and mode:
            return source, destination, mode
        else:
            return None

    def oracle_table_stats(self, owner, table):
        """ Retrieve table stats for owner.table table.
        """
        assert isinstance(owner, str), 'owner must be a string'
        assert isinstance(table, str), 'table must be a string'
        self._debug((owner, table))

        offload_source_table = self._get_offload_source_table(owner, table)
        stats = offload_source_table.table_stats
        self._debug(stats)

        return stats if stats else []

    def backend_ddl(self, owner, table):
        """ Generate DDL for backend table or view.
        """
        assert isinstance(owner, str), 'owner must be a string'
        assert isinstance(table, str), 'table must be a string'
        self._debug((owner, table))
        ddl = self._backend_api.get_table_ddl(owner, table)
        if ddl:
            self._debug(ddl)
            return ddl
        else:
            return []

    def backend_table_stats(self, owner, table):
        """ Retrieve table stats for owner.name table.
        """
        assert isinstance(owner, str), 'owner must be a string'
        assert isinstance(table, str), 'table must be a string'
        self._debug((owner, table))
        stats = self._backend_api.get_table_and_partition_stats(owner, table, as_dict=True)
        if stats:
            self._debug(stats)
            return stats
        else:
            return []

    def impala_info_log(self, host, port, size=None):
        """ Retrieve impalad.INFO log from Impala Daemon HTTP Server.
        """
        assert isinstance(host, str), 'host must be a string'
        assert isinstance(port, int), 'port must be a integer'
        assert port > 0, 'port must be positive integer'
        if size or size == 0:
            assert isinstance(size, int), 'size must be a integer'
            assert size > 0, 'size must be positive integer'
        self._debug((host, port, size))

        if self._test_raw_conn(host, port):
            self._log('Retrieving impalad.INFO log from: %s://%s:%s' % (self._webui_protocol, host, port), VVERBOSE)
            content = self._get_url('%s://%s:%s/logs' % (self._webui_protocol, host, port))
            if content:
                content = self._update_css_path(content)
                if size:
                    resp = content[-size:]
                else:
                    resp = content
            else:
                resp = None
        else:
            resp = None
        return resp

    def impala_config(self, host, port):
        """ Retrieve config from Impala Daemon HTTP Server.
        """
        assert isinstance(host, str), 'host must be a string'
        assert isinstance(port, int), 'port must be a integer'
        assert port > 0, 'port must be positive integer'
        self._debug((host, port))

        if self._test_raw_conn(host, port):
            self._log('Retrieving config from: %s://%s:%s' % (self._webui_protocol, host, port), VVERBOSE)
            content = self._get_url('%s://%s:%s/varz' % (self._webui_protocol, host, port))
            resp = self._update_css_path(content) if content else None
        else:
            resp = None
        return resp

    def impala_query_log(self, host, port, query_id):
        """ Retrieve query logs from Impala Daemon HTTP Server.
        """
        assert isinstance(host, str), 'host must be a string'
        assert isinstance(port, int), 'port must be a integer'
        assert port > 0, 'port must be positive integer'
        assert isinstance(query_id, str), 'query_id must be a string'
        self._debug((host, port, query_id))

        logs = []
        if self._test_raw_conn(host, port):
            self._log('Retrieving query logs for query_id: %s' % query_id, VVERBOSE)
            for uri in ['query_stmt', 'query_plan_text', 'query_summary', 'query_profile']:
                self._debug(uri)
                content = self._get_url('%s://%s:%s/%s?query_id=%s' % (self._webui_protocol, host, port, uri, query_id))
                if content and not 'Unknown query id: %s' % query_id in content and not 'Query id %s not found' % query_id in content:
                    logs.append(content)

        self._debug('len(logs): %s' % len(logs))
        if [log for log in logs if log]:
            resp = '<br><br>'.join([self._update_css_path(log) for log in logs if log])
        else:
            resp = None
        return resp

    def hs2_log(self, host, port, filename, size=None):
        """ Retrieve filename from HiveServer2 Web URI.
        """
        assert isinstance(host, str), 'host must be a string'
        assert isinstance(port, int), 'port must be a integer'
        assert port > 0, 'port must be positive integer'
        assert isinstance(filename, str), 'filename must be a string'
        if size or size == 0:
            assert isinstance(size, int), 'size must be a integer'
            assert size > 0, 'size must be positive integer'
        self._debug((host, port, filename, size))
        resp = None

        if self._test_raw_conn(host, port):
            self._log('Retrieving %s from: %s:%s' % (filename, host, port), VVERBOSE)
            content = self._get_url('%s://%s:%s/logs/%s' % (self._webui_protocol, host, port, filename))
            if content:
                if size:
                    resp = content[-size:]
                else:
                    resp = content
        return resp

    def hs2_config(self, host, port):
        """ Retrieve config from HiveServer2 Web URI.
        """
        assert isinstance(host, str), 'host must be a string'
        assert isinstance(port, int), 'port must be a integer'
        assert port > 0, 'port must be positive integer'
        self._debug((host, port))
        self._log('Retrieving config from: %s:%s' % (host, port), VVERBOSE)

        if self._test_raw_conn(host, port):
            resp = self._get_url('%s://%s:%s/conf' % (self._webui_protocol, host, port))
        else:
            resp = None
        return resp

    def llap_configuration(self, host, port):
        """ Retrieve LLAP configuration JSON from HiveServer2 Interactive.
        """
        assert isinstance(host, str), 'host must be a string'
        assert isinstance(port, int), 'port must be a integer'
        assert port > 0, 'port must be positive integer'
        self._debug('Retrieving LLAP config from: %s:%s' % (host, port))
        resp = None

        if self._test_raw_conn(host, port):
            conf = self._get_url('%s://%s:%s/llap' % (self._webui_protocol, host, port), 'json')
            try:
                if conf['amInfo']:
                    resp = conf
            except:
                resp = None
        return resp

    def llap_enabled(self):
        """ Check if llap is enabled through the "hive.execution.mode" parameter
        """
        self._debug('Checking if LLAP is enabled')
        hive_execution_mode = self._backend_api.get_session_option('hive.execution.mode')

        return bool(hive_execution_mode == 'llap')

    def yarn_log(self, ssh_user, host, application, pattern=None, size=None):
        """ Retrieve YARN logs for an application (and optionally a pattern).
        """
        assert isinstance(ssh_user, str), 'ssh_user must be a string'
        assert isinstance(host, str), 'host must be a string'
        assert isinstance(application, str), 'application must be a string'
        if pattern:
            assert isinstance(pattern, str), 'pattern must be a string'
        if size or size == 0:
            assert isinstance(size, int), 'size must be a integer'
            assert size > 0, 'size must be positive integer'
        self._debug((ssh_user, host, application, pattern, size))

        yarn_cmd = ssh_cmd_prefix(ssh_user, host)
        self._debug(yarn_cmd)
        yarn_cmd.extend(['yarn', 'logs', '-applicationId', application])
        self._debug(yarn_cmd)
        if pattern:
            yarn_cmd.extend(['-log_files_pattern', '%s*' % pattern])
        if size:
            yarn_cmd.extend(['-size', '-%d' % size])
        self._debug(yarn_cmd)

        return self._os_call(yarn_cmd)

    def spark_history_server_address(self, spark_history_server, spark_thrift_host, spark_thrift_port, spark_history_port):
        """ Determine the history server address:
             1. If SPARK_HISTORY_SERVER is set use this
             2. If SPARK_HISTORY_SERVER not set then make a call to SPARK_THRIFT_HOST:SPARK_THRIFT_PORT and
                retrieve the value of the spark.yarn.historyServer.address parameter
             3. If this value is None, or the call in 2 fails, then assume the default value is in use
        """
        assert isinstance(spark_thrift_host, str), 'spark_thrift_host must be a string'
        assert isinstance(spark_thrift_port, int), 'spark_thrift_port must be a integer'
        assert spark_thrift_port > 0, 'spark_thrift_port must be positive integer'
        assert isinstance(spark_history_port, int), 'spark_history_port must be a integer'
        assert spark_history_port > 0, 'spark_history_port must be positive integer'
        self._debug('Determining Spark history server address')
        if spark_history_server:
            return spark_history_server
        else:
            try:
                backend_options = copy.copy(self._orchestration_config)
                backend_options.offload_transport_spark_thrift_host = spark_thrift_host
                backend_options.offload_transport_spark_thrift_port = spark_thrift_port
                spark_api = backend_api_factory(DBTYPE_SPARK, backend_options, self._messages, dry_run=True)
                spark_yarn_historyserver_address = spark_api.get_session_option('spark.yarn.historyServer.address')

                if spark_yarn_historyserver_address:
                    return spark_yarn_historyserver_address
                else:
                    return '%s://%s:%s/' % (self._webui_protocol, spark_thrift_host, spark_history_port)
            except Exception as exc:
                self._debug(traceback.format_exc())
                return '%s://%s:%s/' % (self._webui_protocol, spark_thrift_host, spark_history_port)


    def spark_application_history(self, spark_history_server_address, spark_historic_application_days):
        """ Get list of all Spark applications from the API in the past SPARK_HISTORIC_APPLICATION_DAYS days
            ( e.g. /applications/?minDate=2015-02-03T16:42:40.000GMT )
        """
        spark_history_server = urllib.parse.urlparse(spark_history_server_address)
        assert isinstance(spark_history_server.hostname, str), 'spark_history_server.hostname must be a string'
        assert isinstance(spark_history_server.port, int), 'spark_history_server.port must be a integer'
        assert spark_history_server.port > 0, 'spark_history_server.port must be positive integer'
        assert isinstance(spark_historic_application_days, int), 'spark_historic_application_days must be a integer'
        assert spark_historic_application_days > -1, 'spark_historic_application_days must not be negative'
        self._debug((spark_history_server.hostname, spark_history_server.port, spark_historic_application_days))

        days_back = datetime.today() - timedelta(days=spark_historic_application_days)
        date_back = days_back.strftime('%Y-%m-%d')

        if self._test_raw_conn(spark_history_server.hostname, spark_history_server.port):
            url = '%s://%s:%s/api/v1/applications?minDate=%s' % (self._webui_protocol, spark_history_server.hostname, spark_history_server.port, date_back)
            self._log('Retrieving list of applications from: %s' % url, VVERBOSE)
            content = self._get_url(url)
            try:
                applications = json.loads(content)
                self._log('Retrieved %d application(s)' % len(applications), VVERBOSE)
                resp = [application['id'] for application in applications]
            except:
                self._log('Unable to retrieve list of applications from: %s' % url, VVERBOSE)
                resp = []
        else:
            resp = []
        return resp

    def spark_application_log(self, spark_history_server_address, spark_application_id):
        """ Retrieve the logs from the API ( /applications/[app-id]/stages ).
        """
        spark_history_server = urllib.parse.urlparse(spark_history_server_address)
        assert isinstance(spark_history_server.hostname, str), 'spark_history_server.hostname must be a string'
        assert isinstance(spark_history_server.port, int), 'spark_history_server.port must be a integer'
        assert spark_history_server.port > 0, 'spark_history_server.port must be positive integer'
        assert isinstance(spark_application_id, str), 'spark_application_id must be a string'
        self._debug((spark_history_server.hostname, spark_history_server.port, spark_application_id))

        if self._test_raw_conn(spark_history_server.hostname, spark_history_server.port):
            url = '%s://%s:%s/api/v1/applications/%s/stages' % (self._webui_protocol, spark_history_server.hostname, spark_history_server.port, spark_application_id)
            self._log('Retrieving log of application from: %s' % url, VVERBOSE)
            resp = self._get_url(url)
        else:
            resp = None
        return resp

    def spark_application_config(self, spark_history_server_address, spark_application_id):
        """ Retrieve the config from the API ( /applications/[app-id]/environment ).
        """
        spark_history_server = urllib.parse.urlparse(spark_history_server_address)
        assert isinstance(spark_history_server.hostname, str), 'spark_history_server.hostname must be a string'
        assert isinstance(spark_history_server.port, int), 'spark_history_server.port must be a integer'
        assert spark_history_server.port > 0, 'spark_history_server.port must be positive integer'
        assert isinstance(spark_application_id, str), 'spark_application_id must be a string'
        self._debug((spark_history_server.hostname, spark_history_server.port, spark_application_id))

        if self._test_raw_conn(spark_history_server.hostname, spark_history_server.port):
            url = '%s://%s:%s/api/v1/applications/%s/environment' % (self._webui_protocol, spark_history_server.hostname, spark_history_server.port, spark_application_id)
            self._log('Retrieving environment of application from: %s' % url, VVERBOSE)
            resp = self._get_url(url)
        else:
            resp = None
        return resp

    def bq_query_log(self, query_id):
        assert query_id
        try:
            log_response = self._backend_api.diagnose_query_log(query_id=query_id)
            if not log_response:
                raise DiagnoseException('No query log returned from backend system')
            resp = self._common_tuple_log_response(log_response)
        except BackendApiException as exc:
            self._log(str(exc), VVERBOSE)
            resp = []
        return resp

    def snowflake_query_log(self, session_id, query_id):
        assert session_id and query_id
        try:
            log_response = self._backend_api.diagnose_query_log(session_id=session_id, query_id=query_id)
            resp = self._common_tuple_log_response(log_response)
        except BackendApiException as exc:
            self._log(str(exc), VVERBOSE)
            resp = []
        return resp

    def synapse_configuration(self):
        try:
            config_response = self._backend_api.diagnose_backend_config()
            resp = self._common_tuple_log_response(config_response)
        except BackendApiException as exc:
            self._log(str(exc), VVERBOSE)
            resp = []
        return resp

    def synapse_query_log(self, query_id):
        assert query_id
        try:
            log_response = self._backend_api.diagnose_query_log(query_id=query_id)
            resp = self._common_tuple_log_response(log_response)
        except BackendApiException as exc:
            self._log(str(exc), VVERBOSE)
            resp = []
        return resp
