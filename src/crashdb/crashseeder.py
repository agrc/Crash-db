#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
crashseeder
----------------------------------
the crashseeder module
'''

import csv
import glob
import json
import re
import timeit
from os import makedirs, remove
from os.path import basename, dirname, join, splitext
from shutil import copy, copyfile, rmtree
from time import strftime

import arcpy
from google import oauth2
from google.cloud import storage

from . import secrets
from .models import Lookup, Schema
from .services import BrickLayer, Caster


class CrashSeeder(object):

    def __init__(self, logger):
        self.logger = logger

    def process(self, location, who):
        self.logger.info('starting at {}'.format(strftime('%c')))

        creds = secrets.dev
        if who == 'stage':
            creds = secrets.stage
        elif who == 'prod':
            creds = secrets.prod

        credentials = oauth2.service_account.Credentials.from_service_account_file(self.make_absolute([creds['service_account_key_file']]))
        self.storage_client = storage.Client(credentials=credentials)

        self.brick_layer = BrickLayer(self.logger, creds=creds)
        files = self._get_files(location)
        self.logger.info(files)
        self.truncate_tables(creds)

        #: seed feature type
        self.brick_layer.seed_features(arcpy, create=True)

        for file in files:
            self.logger.info('processing {}'.format(file))

            file_name = splitext(basename(file))[0]
            table_name = self._get_table_name(file_name)
            start = timeit.default_timer()
            items = []

            #: remove null bytes from csv
            fi = open(file, 'r')
            data = fi.read()
            fi.close()

            fo = open(file, 'w')
            data = data.replace('\xff', '')
            data = data.replace('\xfe', '')
            fo.write(data.replace('\x00', ''))
            fo.close()

            with open(file, 'r') as csv_file:
                reader = csv.DictReader(csv_file, delimiter='\t', quoting=csv.QUOTE_MINIMAL)

                for row in reader:
                    new_row = self._etl_row(table_name, row)

                    if new_row is None:
                        continue

                    items.append(new_row)

            self.brick_layer.insert_rows(table_name, items)
            items = []

            end = timeit.default_timer()
            self.logger.info('processing time: {}'.format(end - start))

        #: delete seeded feature
        self.brick_layer.seed_features(arcpy, create=False)

        self.create_dates_js(creds)
        self.create_points_json(creds)
        self.place_files(creds['bucket_name'])

        self.logger.info('finished')

    def get_lengths(self, location):
        files = self._get_files(location)
        items = {'crash': {}}

        for csv_file in files:
            file_name = splitext(basename(csv_file))[0]
            table_name = self._get_table_name(file_name)

            with open(csv_file, 'r') as csv_file:
                reader = csv.DictReader(csv_file)

                if table_name == 'crash':
                    lookup = Schema.crash
                else:
                    continue

                try:
                    for row in reader:
                        for key in list(row.keys()):
                            if key not in list(lookup.keys()):
                                continue
                            if lookup[key]['type'] != 'string':
                                continue

                            if items[table_name].setdefault(key, 0) >= len(row[key]):
                                continue

                            items[table_name][key] = len(row[key])
                except:
                    self.logger.info(csv_file)
                    raise

        import pprint
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(items)

        return items

    def _get_files(self, location):
        if not location:
            raise Exception('Pass in a location containing csv files to import.')

        files = glob.glob(join(location, '*.csv'))

        if len(files) < 1:
            raise Exception(location, 'No csv files found.')

        script_dir = self.make_absolute(['data', 'local'])

        rmtree(script_dir, ignore_errors=True)
        makedirs(script_dir)

        def copy_files_local(file):
            copy(file, script_dir)

        list(map(copy_files_local, files))

        files = glob.glob(join(script_dir, '*.csv'))
        regex = r'(crash|drivers|rollups)_\d{4}'

        return [f for f in files if re.search(regex, f)]

    def _etl_row(self, table_name, row):
        if table_name == 'crash':
            input_keys = Schema.crash_input_keys
            etl_keys = Schema.crash_etl_keys
            lookup = Schema.crash
            formatter = Schema.crash_schema_ordering
        elif table_name == 'driver':
            input_keys = Schema.driver_input_keys
            etl_keys = Schema.driver_etl_keys
            lookup = Schema.driver
            formatter = Schema.driver_schema_ordering
        elif table_name == 'rollup':
            input_keys = Schema.rollup_input_keys
            etl_keys = Schema.rollup_etl_keys
            lookup = Schema.rollup
            formatter = Schema.rollup_schema_ordering
        else:
            self.logger.warn('Not a part of the crash, drivers, rollops convention at %s', table_name)

            return None

        return self._etl_row_generic(row, lookup, input_keys, etl_keys, formatter)

    def _etl_row_generic(self, row, lookup, input_keys, etl_keys, formatter=None):
        etl_row = dict.fromkeys(etl_keys)

        for key in list(row.keys()):
            if key not in input_keys:
                continue

            etl_info = lookup[key]

            value = row[key]
            etl_value = Caster.cast(value, etl_info['type'])

            if 'lookup' in list(etl_info.keys()):
                lookup_name = etl_info['lookup']
                values = Lookup.__dict__[lookup_name]

                if etl_value in list(values.keys()):
                    etl_value = values[etl_value]

            etl_row[etl_info['map']] = etl_value

        if formatter:
            return formatter(etl_row)

        return etl_row

    def _get_table_name(self, file_name):
        if 'crash' in file_name:
            return 'crash'
        elif 'driver' in file_name:
            return 'driver'
        elif 'rollup' in file_name:
            return 'rollup'
        else:
            return None

    def create_database(self, where, who):
        self.logger.info('DO NOT FORGET TO UPDATE THE POINTS.JSON')

        sr = arcpy.SpatialReference(26912)

        #: create sql drive and rollup tables
        with open(self.make_absolute([where[0]]), 'r') as f:
            sql = f.read()

        creds = secrets.dev
        if who == 'stage':
            creds = secrets.stage
        elif who == 'prod':
            creds = secrets.prod

        sde = self.make_absolute(['connections', creds['sde_connection_path']])

        self.logger.info('connecting to {} database'.format(who))

        try:
            c = arcpy.ArcSDESQLExecute(sde)
            c.execute(sql)
        except Exception as e:
            raise e
        finally:
            if c:
                del c

        self.logger.info('created sql tables')

        arcpy.env.overwriteOutput = True

        self.logger.info('creating spatial tables')
        try:
            arcpy.CreateFeatureclass_management(sde, 'CrashLocation', 'POINT', spatial_reference=sr)

        except arcpy.ExecuteError as e:
            if 'ERROR 000258' in str(e):
                self.logger.info('feature class exists. Deleting and trying again.')
                arcpy.Delete_management(join(sde, 'CrashLocation'))

                arcpy.CreateFeatureclass_management(sde, 'CrashLocation', 'POINT', spatial_reference=sr)

        self.logger.info('adding spatial table fields')
        arcpy.env.workspace = sde

        #: name, type, null, length
        fields = [['crash_id', 'LONG', 'NON_NULLABLE'], ['crash_date', 'DATE', 'NULLABLE'], ['crash_year', 'SHORT', 'NULLABLE'],
                  ['crash_month', 'SHORT', 'NULLABLE'], ['crash_day', 'SHORT', 'NULLABLE'], ['crash_hour', 'SHORT', 'NULLABLE'],
                  ['crash_minute', 'SHORT', 'NULLABLE'], ['construction', 'SHORT', 'NULLABLE'], ['weather_condition', 'TEXT', 'NULLABLE', 50],
                  ['road_condition', 'TEXT', 'NULLABLE', 50], ['event', 'TEXT', 'NULLABLE', 100], ['collision_type', 'TEXT', 'NULLABLE', 50],
                  ['severity', 'TEXT', 'NULLABLE', 50], ['case_number', 'TEXT', 'NULLABLE', 400], ['officer_name', 'TEXT', 'NULLABLE', 100],
                  ['officer_department', 'TEXT', 'NULLABLE', 100], ['road_name', 'TEXT', 'NULLABLE', 100], ['road_type', 'TEXT', 'NULLABLE', 20],
                  ['route_number', 'LONG', 'NULLABLE'], ['milepost', 'DOUBLE', 'NULLABLE'], ['city', 'TEXT', 'NULLABLE', 50],
                  ['county', 'TEXT', 'NULLABLE', 25], ['utm_x', 'DOUBLE', 'NULLABLE'], ['utm_y', 'DOUBLE', 'NULLABLE']]

        for field in fields:
            self.add_field('CrashLocation', field)

        self.logger.info('granting read access')

        with open(self.make_absolute(['data', 'sql', 'grant_permissions.sql']), 'r') as f:
            sql = f.read()

        try:
            c = arcpy.ArcSDESQLExecute(sde)
            c.execute(sql)
        except Exception as e:
            raise e
        finally:
            del c

    def add_field(self, table, field):
        if len(field) == 4:
            arcpy.AddField_management(table, field[0], field[1], field_is_nullable=field[2], field_length=field[3])

            return

        arcpy.AddField_management(table, field[0], field[1], field_is_nullable=field[2])

    def truncate_tables(self, creds):
        sde = self.make_absolute(['connections', creds['sde_connection_path']])

        with open(self.make_absolute(['data', 'sql', 'truncate.sql']), 'r') as f:
            sql = f.read()

        self.logger.info('truncating tabular tables')
        try:
            c = arcpy.ArcSDESQLExecute(sde)
            c.execute(sql)
        except Exception as e:
            raise e
        finally:
            if c is not None:
                del c

        arcpy.env.overwriteOutput = True
        arcpy.env.workspace = sde

        self.logger.info('truncating spatial tables')
        try:
            arcpy.TruncateTable_management('CrashLocation')

        except arcpy.ExecuteError as e:
            self.logger.info(e)

    def create_dates_js(self, creds):
        self.logger.info('creating dates.json')
        start = timeit.default_timer()
        sde = self.make_absolute(['connections', creds['sde_connection_path']])

        sql = '''SELECT MAX(CAST(crash_date as varchar)) as max_date, MIN(CAST(crash_date as varchar)) as min_date
        FROM [DDACTS].[DDACTSadmin].[CRASHLOCATION]'''

        try:
            c = arcpy.ArcSDESQLExecute(sde)
            max_min = c.execute(sql)
            max_min = max_min[0]
        except Exception as e:
            self.logger.info(e)
            raise e
        finally:
            if c:
                del c

        with open(self.make_absolute(['pickup', 'dates.json']), 'w') as outfile:
            template = '{{"minDate": "{}", "maxDate": "{}"}}'.format(max_min[1].split(' ')[0], max_min[0].split(' ')[0])

            outfile.write(template)

        self.logger.info('processing time: {}'.format(timeit.default_timer() - start))

    def create_points_json(self, creds):
        self.logger.info('creating new points.json')

        start = timeit.default_timer()

        sde = self.make_absolute(['connections', creds['sde_connection_path']])

        pattern = re.compile(r'\s+')
        points = {'points': []}

        sql = 'SELECT [OBJECTID],[Shape].STX as x,[Shape].STY as y FROM [DDACTS].[DDACTSadmin].[CRASHLOCATION]'

        try:
            c = arcpy.ArcSDESQLExecute(sde)
            result = c.execute(sql)
        except Exception as e:
            self.logger.info(e)
            raise e
        finally:
            if c:
                del c

        def append_point(crash):
            id, x, y = crash
            if x is None or y is None:
                return

            x = int(x)
            y = int(y)

            points['points'].append([id, x, y])

        with open(self.make_absolute(['pickup', 'points.json']), 'w') as outfile:
            list(map(append_point, result))

            content = re.sub(pattern, '', json.dumps(points))
            outfile.write(content)

            end = timeit.default_timer()
            self.logger.info('processing time: {}'.format(end - start))

    def place_files(self, bucket_name):
        try:
            self.logger.info('uploading points to gcp bucket')
            self._upload_blob(bucket_name, self.make_absolute(['pickup', 'points.json']), 'points.json')
        except Exception as e:
            self.logger.error('could not upload new points')

        try:
            self.logger.info('uploading dates to gcp bucket')
            self._upload_blob(bucket_name, self.make_absolute(['pickup', 'dates.json']), 'dates.json')
        except Exception as e:
            self.logger.error('could not upload new dates')

    def _upload_blob(self, bucket_name, source_file_name, destination_blob_name):
        """Uploads a file to the bucket."""

        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_name)

    def make_absolute(self, fragments):
        parent = dirname(__file__)

        return join(parent, *fragments)
