#!/usr/bin/env python
# * coding: utf8 *
'''
CrashPallet.py

A module that contains the pallet to keep the crashmapping.utah.gov website fresh
'''

from subprocess import CalledProcessError, check_call
from time import strftime

from forklift.models import Pallet

from crashdb import secrets
from crashdb.crashseeder import CrashSeeder


class CrashPallet(Pallet):

    def __init__(self, configuration=None):
        super(CrashPallet, self).__init__()

        #: this should not be used by LightSwitch but it is here for documentation purposes.
        self.arcgis_services = [('Crash/Crashes', 'MapServer')]

    def build(self, configuration):
        if configuration is None:
            self.creds = secrets.prod
            self.configuration = 'prod'
            self.is_ready_to_ship = lambda: True

            return

        if configuration == 'Dev':
            self.creds = secrets.dev
            self.is_ready_to_ship = lambda: True
            self.configuration = 'dev'
        elif configuration == 'Staging':
            self.creds = secrets.stage
            self.configuration = 'stage'
        elif configuration == 'Production':
            self.creds = secrets.prod
            self.configuration = 'prod'

    def is_ready_to_ship(self):
        ready = strftime('%A') == 'Monday'
        if not ready:
            self.success = (True, 'This pallet only runs on Monday.')

        return ready

    def ship(self):
        self.log.info('mounting U drive')
        error = None

        try:
            check_call(['net', 'use', 'U:', r'\\ftp.utah.gov\agrcftp', self.creds['mount_password'], '/USER:{}'.format(self.creds['mount_user']),
                        '/PERSISTENT:YES'])
        except CalledProcessError as e:
            self.log.error('There was a problem mounting the drive %s', e.message, exc_info=True)

        try:
            dbseeder = CrashSeeder(self.log)
            dbseeder.process('U:/collision', self.configuration)
        except Exception as e:
            self.log.error('There was a problem shipping CrashPallet. %s', e, exc_info=True)
            error = e

        try:
            check_call(['net', 'use', '/delete', 'U:'])
        except CalledProcessError as e:
            self.log.error('There was a problem unmounting the drive %s', e, exc_info=True)

        if error is not None:
            raise error
