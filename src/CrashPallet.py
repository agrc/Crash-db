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


class CrashPallet(Pallet):

    def __init__(self):
        super(CrashPallet, self).__init__()

        #: this should not be used by LightSwitch but it is here for documentation purposes.
        self.arcgis_services = [('Crash/Crashes', 'MapServer')]

    def is_ready_to_ship(self):
        #: if today is monday, run
        return True

    def ship(self):
        self.log.info('mounting U drive')
        error = None

        try:
            check_call(['net', 'use', 'U:', r'\\ftp.utah.gov\agrcftp', secrets.stage['mount_password'], '/USER:smbagrc',
                        '/PERSISTENT:YES'])
        except CalledProcessError as e:
            self.log.error('There was a problem mounting the drive %s', e.message, exc_info=True)

        try:
            dbseeder = DbSeeder(self.log)
            import pdb; pdb.set_trace()
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
