#!/usr/bin/env python
# * coding: utf8 *
'''
CrashPallet.py

A module that contains the pallet to keep the crashmapping.utah.gov website fresh
'''

import os
import re
from glob import glob
import pysftp
import logging
from random import uniform
from shutil import rmtree
from subprocess import CalledProcessError, check_call
from time import sleep, strftime

import httplib2
from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from crashdb import secrets
from crashdb.crashseeder import CrashSeeder
from forklift.models import Pallet
from google.oauth2 import service_account

SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = 'ddacts-oauth.json'


class CrashPallet(Pallet):

    def __init__(self, configuration=None):
        super(CrashPallet, self).__init__()

        #: this should not be used by LightSwitch but it is here for documentation purposes.
        self.arcgis_services = [('Crash/Crashes', 'MapServer')]

    def build(self, configuration):
        logging.getLogger('googleapicliet.discovery_cache').setLevel(logging.ERROR)

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

    def _download_files(self):
        ephemeral = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'ftp_data')

        error = None
        if os.path.exists(ephemeral):
            rmtree(ephemeral)

        os.makedirs(ephemeral)

        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

        self.log.info('downloading ftp data')
        error = None

        try:
            with pysftp.Connection(
                host=self.creds['ftp_host'], username=self.creds['ftp_username'], password=self.creds['ftp_password'], cnopts=cnopts
            ) as sftp:

                sftp.chdir(self.creds['ftp_directory'])
                items = sftp.listdir()

                self.log.debug('acting on {}'.format(','.join(items)))

                csvs = [item for item in items if self.keep_file(item)]

                self.log.debug('filtered to {}'.format(','.join(csvs)))

                for file_path in csvs:
                    self.log.debug('downloading {}'.format(file_path))
                    sftp.get(file_path, os.path.join(ephemeral, os.path.basename(file_path)), preserve_mtime=True)
        except Exception as e:
            self.log.error('there was a problem with the ftp', e)
            error = e

        return ephemeral

    def refresh_drive_crash_download(self, ephemeral):
        self.log.info('uploading file to drive')

        try:
            files = glob(os.path.join(ephemeral, '*.csv'))
            regex = r'(download)'
            download_file = [f for f in files if re.search(regex, f)]

            if not download_file or len(download_file) != 1:
                self.log.warn('not uploading drive file')

                return

            upload_file = download_file[0]

            parent = os.path.dirname(__file__)
            secrets = os.path.join(parent, SERVICE_ACCOUNT_FILE)

            credentials = service_account.Credentials.from_service_account_file(secrets, scopes=SCOPES)

            service = build('drive', 'v3', credentials=credentials)

            media_body = MediaFileUpload(upload_file, mimetype='text/csv', resumable=True)
            service.files().update(fileId='18a9jKmFbq2_0zvY9aN5jdMpof5gE0xSG', supportsTeamDrives=True, media_body=media_body).execute()

            self.log.info('upload finished')

            return True
        except Exception as e:
            self.log.error('The download was not updated %s', e, exc_info=True)

            return False

    def keep_file(self, file_path):
        _, ext = os.path.splitext(os.path.basename(file_path))

        return ext.lower() == '.csv'

    def ship(self):
        ephemeral = self._download_files()
        error = None

        try:
            dbseeder = CrashSeeder(self.log)
            dbseeder.process(ephemeral, self.configuration)
        except Exception as e:
            self.log.error('There was a problem shipping CrashPallet. %s', e, exc_info=True)
            error = e

        try:
            rmtree(ephemeral)
        except Exception as e:
            self.log.warn('Could not delete the ephemeral directory %s', e, exc_info=True)

        if error is not None:
            raise error

if __name__ == "__main__":
    '''
    optional arguments:
    1 - forklift configuration (Production | Dev)
        Defaults to Dev.
    2 - action: (download | ship)
        Defaults to ship
    '''
    import sys

    try:
        config = sys.argv[1]
    except IndexError:
        config = 'Dev'

    try:
        action = sys.argv[2]
    except IndexError:
        action = None

    pallet = CrashPallet()
    pallet.configure_standalone_logging()
    pallet.build(config)

    if action and action == 'download':
        ephemeral = pallet._download_files()


        if pallet.refresh_drive_crash_download(ephemeral):
            print('download completed')
            sys.exit()

        print('drive refresh failed')
        sys.exit()

    pallet.ship()
    print(pallet.success)
