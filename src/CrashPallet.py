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
from random import uniform
from shutil import rmtree
from subprocess import CalledProcessError, check_call
from time import sleep, strftime

import httplib2
from apiclient import discovery, errors
from apiclient.http import MediaFileUpload
from crashdb import secrets
from crashdb.crashseeder import CrashSeeder
from forklift.models import Pallet
from oauth2client import client, tools
from oauth2client.file import Storage
from oauth2client.service_account import ServiceAccountCredentials

SCOPES = 'https://www.googleapis.com/auth/drive'
SERVICE_ACCOUNT_SECRET_FILE = 'ddacts-oauth.json'

#: oauth2
APPLICATION_NAME = 'ddacts-download'

flags = None


class APIS(object):
    drive = ('drive', 'v3')
    sheets = ('sheets', 'v4')


class flags_shim(object):

    def __init__(self):
        self.auth_host_name = 'localhost'
        self.noauth_local_webserver = False
        self.auth_host_port = [8080, 8090]
        self.logging_level = 'ERROR'


class AgrcDriver(object):
    def __init__(self, api_service):
        self.service = api_service

    def update_file(self, file_id, local_file, mime_type):
        media_body = MediaFileUpload(local_file,
                                     mimetype=mime_type,
                                     resumable=True)

        request = self.service.files().update(fileId=file_id,
                                              supportsTeamDrives=True,
                                              media_body=media_body)

        response = None
        backoff = 1
        while response is None:
            try:
                _, response = request.next_chunk()
            except errors.HttpError as e:
                if e.resp.status in [404]:  # TODO restart on 410 gone
                    # Start the upload all over again.
                    raise Exception('Upload Failed 404')
                elif e.resp.status in [500, 502, 503, 504]:
                    if backoff > 8:
                        raise Exception('Upload Failed: {}'.format(e))
                    print('Retrying upload in: {} seconds'.format(backoff))
                    sleep(backoff + uniform(.001, .999))
                    backoff += backoff
                else:
                    msg = 'Upload Failed \n{}'.format(e)
                    raise Exception(msg)

        return response.get('id')


class ApiService(object):

    def __init__(self, apis, secrets=SERVICE_ACCOUNT_SECRET_FILE, scopes=SCOPES, use_oauth=False):
        self.services = []
        for api_name, api_version in apis:
            if use_oauth:
                self.services.append(self.setup_oauth_service(secrets, scopes, api_name, api_version))
            else:
                self.services.append(self.setup_account_service(secrets, scopes, api_name, api_version))

    def get_oauth_credentials(self, secrets, scopes, application_name=APPLICATION_NAME, flags=flags):
        '''
        Get valid user credentials from storage.
        If nothing has been stored, or if the stored credentials are invalid,
        the OAuth2 flow is completed to obtain the new credentials.
        Returns:
            Credentials, the obtained credential.
        '''
        home_dir = os.path.expanduser('~')
        credential_dir = os.path.join(home_dir, '.credentials')

        if not os.path.exists(credential_dir):
            os.makedirs(credential_dir)
        credential_path = os.path.join(credential_dir, 'ddacts-uploader.json')

        store = Storage(credential_path)
        credentials = store.get()

        if not credentials or credentials.invalid:
            flow = client.flow_from_clientsecrets(secrets, scopes)
            flow.user_agent = application_name

            if flags is None:
                flags = flags_shim()

            credentials = tools.run_flow(flow, store, flags)
            print('Storing credentials to ' + credential_path)

        return credentials

    def get_credentials(self, secrets, scopes):
        '''Get service account credentials from json key file.'''
        credentials = ServiceAccountCredentials.from_json_keyfile_name(secrets, scopes)

        return credentials

    def setup_oauth_service(self, secrets, scopes, api_name, api_version):
        credentials = self.get_oauth_credentials(secrets, scopes)
        http = credentials.authorize(httplib2.Http())
        service = discovery.build(api_name, api_version, http=http)

        return service

    def setup_account_service(self, secrets, scopes, api_name, api_version):
        # get auth
        credentials = self.get_credentials(secrets, scopes)
        http = credentials.authorize(httplib2.Http())
        service = discovery.build(api_name, api_version, http=http)

        return service


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

    def refresh_drive_crash_download(self, files):
        parent = os.path.dirname(__file__)
        secrets = os.path.join(parent, SERVICE_ACCOUNT_SECRET_FILE)
        api_services = ApiService((APIS.drive,), secrets=secrets, scopes=SCOPES)
        drive_service = AgrcDriver(api_services.services[0])

        drive_service.update_file('18a9jKmFbq2_0zvY9aN5jdMpof5gE0xSG', files, 'text/csv')

    def keep_file(self, file_path):
        _, ext = os.path.splitext(os.path.basename(file_path))

        return ext.lower() == '.csv'

    def ship(self):
        ephemeral = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'ftp_data')

        if os.path.exists(ephemeral):
            rmtree(ephemeral)

        os.makedirs(ephemeral)

        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

        self.log.info('downloading ftp data')
        error = None

        try:
            with pysftp.Connection(
                     host=self.creds['ftp_host'],
                     username=self.creds['ftp_username'],
                     password=self.creds['ftp_password'],
                     cnopts=cnopts) as sftp:

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

        try:
            dbseeder = CrashSeeder(self.log)
            dbseeder.process(ephemeral, self.configuration)
        except Exception as e:
            self.log.error('There was a problem shipping CrashPallet. %s', e, exc_info=True)
            error = e

        try:
            files = glob(os.path.join(ephemeral, '*.csv'))
            regex = r'(download)'
            download_file = [f for f in files if re.search(regex, f)]

            if download_file and len(download_file) == 1:
                self.refresh_drive_crash_download(download_file[0])
        except Exception as e:
            self.log.error('The download was not updated %s', e, exc_info=True)
            error = e

        try:
            rmtree(ephemeral)
        except Exception as e:
            self.log.error('Could not delete the ephemeral directory %s', e, exc_info=True)
            error = e

        if error is not None:
            raise error
