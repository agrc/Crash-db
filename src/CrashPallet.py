#!/usr/bin/env python
# * coding: utf8 *
'''
CrashPallet.py

A module that contains the pallet to keep the crashmapping.utah.gov website fresh
'''

from dbseeder import secrets
from dbseeder.dbseeder import DbSeeder
from forklift.models import Pallet
from subprocess import CalledProcessError
from subprocess import check_call
from time import strftime


class CrashPallet(Pallet):

    def __init__(self, configuration=None):
        super(CrashPallet, self).__init__()

        #: this should not be used by LightSwitch but it is here for documentation purposes.
        self.arcgis_services = [('Crash/Crashes', 'MapServer')]

        if configuration is None:
            self.creds = secrets.prod
            return

        if configuration == 'dev':
            self.creds = secrets.dev
        elif configuration == 'stage':
            self.creds = secrets.stage
        elif configuration == 'prod':
            self.creds = secrets.prod

        #: if running the pallet with a configuration we probably don't care if it's a monday so shipit
        self.is_ready_to_ship = lambda: True

    def is_ready_to_ship(self):
        return strftime('%A') == 'Monday'

    def ship(self):
        self.log.info('mounting U drive')
        error = None

        try:
            check_call(['net', 'use', 'U:', r'\\ftp.utah.gov\agrcftp', self.creds['mount_password'], '/USER:smbagrc',
                        '/PERSISTENT:YES'])
        except CalledProcessError as e:
            self.log.error('There was a problem mounting the drive %s', e.message, exc_info=True)

        try:
            dbseeder = DbSeeder(self.log)
            dbseeder.process('U:/collision', 'stage')
        except Exception as e:
            self.log.error('There was a problem shipping CrashPallet. %s', e.message, exc_info=True)
            error = e

        try:
            check_call(['net', 'use', '/delete', 'U:'])
        except CalledProcessError as e:
            self.log.error('There was a problem unmounting the drive %s', e.message, exc_info=True)

        if error is not None:
            raise error
