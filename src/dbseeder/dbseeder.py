#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
dbseeder
----------------------------------
the dbseeder module
'''

import arcpy
import csv
import decimal
import glob
import json
import os
import re
import secrets
import timeit
from models import Schema, Lookup
from os.path import basename, splitext, join, sep
from os import environ, makedirs, remove
from services import Caster, BrickLayer
from shutil import copy, copyfile, rmtree
from time import strftime


class DbSeeder(object):

    def __init__(self):
        super(DbSeeder, self).__init__()

    def process(self, location, who):
        print('started at {}'.format(strftime('%c')))

        creds = secrets.dev
        if who == 'stage':
            creds = secrets.stage
        elif who == 'prod':
            creds = secrets.prod

        self.brick_layer = BrickLayer(creds=creds)
        files = self._get_files(location)

        self.truncate_tables(creds)

        #: seed feature type
        self.brick_layer.seed_features(arcpy, create=True)

        for file in files:
            print 'processing {}'.format(file)

            file_name = splitext(basename(file))[0]
            table_name = self._get_table_name(file_name)
            start = timeit.default_timer()
            items = []

            #: remove null bytes from csv
            fi = open(file, 'rb')
            data = fi.read()
            fi.close()
            fo = open(file, 'wb')
            data = data.replace('\xff', '')
            data = data.replace('\xfe', '')
            fo.write(data.replace('\x00', ''))

            fo.close()

            with open(file, 'rb') as csv_file:
                reader = csv.DictReader(csv_file, delimiter='\t', quoting=csv.QUOTE_MINIMAL)

                for row in reader:
                    items.append(self._etl_row(table_name, row))

            self.brick_layer.insert_rows(table_name, items)
            items = []

            end = timeit.default_timer()
            print 'processing time: {}'.format(end - start)

        #: delete seeded feature
        self.brick_layer.seed_features(arcpy, create=False)

        self.create_dates_js(creds)
        self.create_points_json(creds)
        self.place_files(who)

        print('finished')

    def get_lengths(self, location):
        files = self._get_files(location)
        items = {
            'crash': {}
        }

        for file in files:
            file_name = splitext(basename(file))[0]
            table_name = self._get_table_name(file_name)

            with open(file, 'r') as csv_file:
                reader = csv.DictReader(csv_file)

                if table_name == 'crash':
                    lookup = Schema.crash
                else:
                    continue

                try:
                    for row in reader:
                        for key in row.keys():
                            if key not in lookup.keys():
                                continue
                            if lookup[key]['type'] != 'string':
                                continue

                            if items[table_name].setdefault(key, 0) >= len(row[key]):
                                continue

                            items[table_name][key] = len(row[key])
                except:
                    print file
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

        script_dir = os.path.dirname(__file__)
        script_dir = join(script_dir, 'data', 'local')

        rmtree(script_dir, ignore_errors=True)
        makedirs(script_dir)

        def copy_files_local(file):
            copy(file, script_dir)

        map(copy_files_local, files)

        files = glob.glob(join(script_dir, '*.csv'))

        return files

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
            raise Exception(file, 'Not a part of the crash, drivers, rollops convention')

        return self._etl_row_generic(row, lookup, input_keys, etl_keys, formatter)

    def _etl_row_generic(self, row, lookup, input_keys, etl_keys, formatter=None):
        etl_row = dict.fromkeys(etl_keys)

        for key in row.keys():
            if key not in input_keys:
                continue

            etl_info = lookup[key]

            value = row[key]
            etl_value = Caster.cast(value, etl_info['type'])

            if 'lookup' in etl_info.keys():
                lookup_name = etl_info['lookup']
                values = Lookup.__dict__[lookup_name]

                if etl_value in values.keys():
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
        print('DO NOT FORGET TO UPDATE THE POINTS.JSON')

        script_dir = os.path.dirname(__file__)
        sr = os.path.join(script_dir, 'data/26912.prj')
        creds = secrets.dev

        #: create sql drive and rollup tables
        with open(os.path.join(script_dir, where[0]), 'r') as f:
            sql = f.read()

        if who == 'stage':
            creds = secrets.stage
        elif who == 'prod':
            creds = secrets.prod

        sde = os.path.join(script_dir,
                           'connections',
                           creds['sde_connection_path'])

        print 'connecting to {} database'.format(who)

        try:
            c = arcpy.ArcSDESQLExecute(sde)
            c.execute(sql)
        except Exception, e:
            raise e
        finally:
            if c:
                del c

        print 'created sql tables'

        arcpy.env.overwriteOutput = True

        print 'creating spatial tables'
        try:
            arcpy.CreateFeatureclass_management(sde, 'CrashLocation', 'POINT',
                                                spatial_reference=sr)

        except arcpy.ExecuteError, e:
            if 'ERROR 000258' in e.message:
                print 'feature class exists. Deleting and trying again.'
                arcpy.Delete_management(os.path.join(sde, 'CrashLocation'))

                arcpy.CreateFeatureclass_management(sde, 'CrashLocation', 'POINT',
                                                    spatial_reference=sr)

        print 'adding spatial table fields'
        arcpy.env.workspace = sde

        #: name, type, null, length
        fields = [
            ['crash_id', 'LONG', 'NON_NULLABLE'],
            ['crash_date', 'DATE', 'NULLABLE'],
            ['crash_year', 'SHORT', 'NULLABLE'],
            ['crash_month', 'SHORT', 'NULLABLE'],
            ['crash_day', 'SHORT', 'NULLABLE'],
            ['crash_hour', 'SHORT', 'NULLABLE'],
            ['crash_minute', 'SHORT', 'NULLABLE'],
            ['construction', 'SHORT', 'NULLABLE'],
            ['weather_condition', 'TEXT', 'NULLABLE', 50],
            ['road_condition', 'TEXT', 'NULLABLE', 50],
            ['event', 'TEXT', 'NULLABLE', 100],
            ['collision_type', 'TEXT', 'NULLABLE', 50],
            ['severity', 'TEXT', 'NULLABLE', 50],
            ['case_number', 'TEXT', 'NULLABLE', 400],
            ['officer_name', 'TEXT', 'NULLABLE', 100],
            ['officer_department', 'TEXT', 'NULLABLE', 100],
            ['road_name', 'TEXT', 'NULLABLE', 100],
            ['road_type', 'TEXT', 'NULLABLE', 20],
            ['route_number', 'LONG', 'NULLABLE'],
            ['milepost', 'DOUBLE', 'NULLABLE'],
            ['city', 'TEXT', 'NULLABLE', 50],
            ['county', 'TEXT', 'NULLABLE', 25],
            ['utm_x', 'DOUBLE', 'NULLABLE'],
            ['utm_y', 'DOUBLE', 'NULLABLE']
        ]

        for field in fields:
            self.add_field('CrashLocation', field)

        print('granting read access')

        with open(os.path.join(script_dir, 'data/sql/grant_permissions.sql'), 'r') as f:
            sql = f.read()

        try:
            c = arcpy.ArcSDESQLExecute(sde)
            c.execute(sql)
        except Exception, e:
            raise e
        finally:
            del c

    def add_field(self, table, field):
        if len(field) == 4:
            arcpy.AddField_management(table, field[0], field[1],
                                      field_is_nullable=field[2],
                                      field_length=field[3])

            return

        arcpy.AddField_management(table, field[0], field[1],
                                  field_is_nullable=field[2])

    def truncate_tables(self, creds):
        script_dir = os.path.dirname(__file__)

        sde = os.path.join(script_dir,
                           'connections',
                           creds['sde_connection_path'])

        with open(os.path.join(script_dir, 'data/sql/truncate.sql'), 'r') as f:
            sql = f.read()

        print('truncating tabular tables')
        try:
            c = arcpy.ArcSDESQLExecute(sde)
            c.execute(sql)
        except Exception, e:
            raise e
        finally:
            if c:
                del c

        arcpy.env.overwriteOutput = True
        arcpy.env.workspace = sde

        print 'truncating spatial tables'
        try:
            arcpy.TruncateTable_management('CrashLocation')

        except arcpy.ExecuteError, e:
            print(e.message)

    def create_dates_js(self, creds):
        print('creating dates.js')
        start = timeit.default_timer()
        script_dir = os.path.dirname(__file__)

        sde = os.path.join(script_dir,
                           'connections',
                           creds['sde_connection_path'])

        sql = '''SELECT max(CAST(CAST(crash_year AS VARCHAR(4)) + RIGHT(\'0\' + CAST(crash_month AS VARCHAR(2)), 2) +
              RIGHT(\'0\' + CAST(crash_day AS VARCHAR(2)), 2) AS DATETIME)) as max_date,
              min(CAST(CAST(crash_year AS VARCHAR(4)) + RIGHT(\'0\' + CAST(crash_month AS VARCHAR(2)), 2) + RIGHT(\'0\' +
              CAST(crash_day AS VARCHAR(2)), 2) AS DATETIME)) as max_date FROM [DDACTS].[DDACTSadmin].[CRASHLOCATION]'''
        try:
            c = arcpy.ArcSDESQLExecute(sde)
            max_min = c.execute(sql)
            max_min = max_min[0]
        except Exception, e:
            print(e)
            raise e
        finally:
            if c:
                del c

        with open('dates.js', 'w') as outfile:
            template = 'define([], function() {{return {{minDate: \'{}\', maxDate: \'{}\'}};}});'.format(max_min[1], max_min[0])

            outfile.write(template)

        print 'processing time: {}'.format(timeit.default_timer() - start)

    def create_points_json(self, creds):
        print('creating new points.json')

        start = timeit.default_timer()
        script_dir = os.path.dirname(__file__)

        sde = os.path.join(script_dir,
                           'connections',
                           creds['sde_connection_path'])

        pattern = re.compile(r'\s+')
        points = {
            'points': []
        }

        sql = 'SELECT [OBJECTID],[Shape].STX as x,[Shape].STY as y FROM [DDACTS].[DDACTSadmin].[CRASHLOCATION]'

        try:
            c = arcpy.ArcSDESQLExecute(sde)
            result = c.execute(sql)
        except Exception, e:
            print(e)
            raise e
        finally:
            if c:
                del c

        def append_point(crash):
            if crash[1] > 0 and crash[2] > 0:
                x = round(crash[1], 2)
                y = round(crash[2], 2)

                dx = decimal.Decimal(x).as_tuple()
                dy = decimal.Decimal(y).as_tuple()

                if dx.exponent == 0:
                    x = int(x)
                if dy.exponent == 0:
                    y = int(y)

                points['points'].append([crash[0], x, y])

        with open('points.json', 'w') as outfile:
            map(append_point, result)

            content = re.sub(pattern, '', json.dumps(points))
            outfile.write(content)

            end = timeit.default_timer()
            print 'processing time: {}'.format(end - start)

    def place_files(self, who):
        place = join(environ.get("HOMEDRIVE"), sep, 'Projects', 'GitHub', 'Crash-web', 'src')

        if who == 'stage':
            place = join(environ.get("HOMEDRIVE"), sep, 'inetpub', 'wwwroot', 'crash')
        elif who == 'prod':
            place = join('w:', sep, 'inetpub', 'wwwroot', 'crash')

        points = join(place, 'points.json')
        dates = join(place, 'app', 'resources', 'dates.js')

        try:
            remove(points)
        except:
            pass

        try:
            remove(dates)
        except:
            pass

        copyfile('points.json', points)
        copyfile('dates.js', dates)
