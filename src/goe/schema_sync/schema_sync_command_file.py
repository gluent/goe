#! /usr/bin/env python3
""" SchemaSyncCommandFile: Library for managing Schema Sync command file
    LICENSE_TEXT
"""
import os
import traceback
from datetime import datetime

from gluent import version

divider = '=' * 100
sub_divider = '-' * 100


class SchemaSyncCommandFileException(Exception): pass

class SchemaSyncCommandFile(object):
    """ Class for managing the Schema Sync command file
    """

    def __init__(self, command_file):
        self._command_file = command_file
        self._commands_written = 0

        try:
            with open(self._command_file, 'w') as cmd_file:
                cmd_file.write('\n%s' % divider)
                cmd_file.write('\nGluent Schema Sync v%s Command File' % version())
                cmd_file.write('\nCopyright 2015-%s Gluent Inc. All rights reserved.' % datetime.now().strftime('%Y'))
                cmd_file.write('\n%s\n' % divider)
        except IOError as exc:
            raise SchemaSyncCommandFileException('Unable to create command file "%s"\n%s' % (self._command_file, traceback.format_exc()))


    def write(self, msg):
        try:
            with open(self._command_file, 'a') as cmd_file:
                cmd_file.write('\n%s' % msg)
        except IOError as exc:
            raise SchemaSyncCommandFileException('Unable to write to command file "%s"\n%s' % (self._command_file, traceback.format_exc()))


    def remove(self):
        try:
            os.remove(self._command_file)
        except OSError as exc:
            raise SchemaSyncCommandFileException('Unable to remove command file "%s"\n%s' % (self._command_file, traceback.format_exc()))


    def write_table_header(self, source_table):
        self.write(sub_divider)
        self.write('Change commands for %s' % source_table)
        self.write(sub_divider)


    def write_command(self, command):
        if command.lstrip().lower().startswith(('#', 'echo', 'hdfs', '${offload_home}', 'bigquery', 'missing')):
            command_suffix = ''
        elif command.lstrip().lower().startswith('create or replace trigger'):
            command_suffix = '\n/'
        else:
            command_suffix = ';'
        self.write('%s%s' % (command.lstrip(), command_suffix))
        self._commands_written += 1


    def commands_in_file(self):
        return self._commands_written
