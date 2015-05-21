#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
dbseeder
----------------------------------
the dbseeder module
'''

import arcpy
import csv
import glob
import os
import timeit
import secrets
from models import Schema, Lookup
from os.path import basename, splitext, join
from services import Caster, BrickLayer


class DbSeeder(object):

    def __init__(self):
        super(DbSeeder, self).__init__()

    def process(self, location, who):
        print('DO NOT FORGET TO UPDATE THE POINTS.JSON')

        creds = secrets.dev
        if who == 'stage':
            creds = secrets.stage
        elif who == 'prod':
            creds = secrets.prod

        self.brick_layer = BrickLayer(creds=creds)
        files = self._get_files(location)

        #: seed feature type
        self.brick_layer.seed_features(arcpy, create=True)

        for file in files:
            print 'processing {}'.format(file)

            file_name = splitext(basename(file))[0]
            table_name = self._get_table_name(file_name)
            start = timeit.default_timer()
            items = []

            with open(file, 'r') as csv_file:
                reader = csv.DictReader(csv_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)

                for row in reader:
                    items.append(self._etl_row(table_name, row))

            self.brick_layer.insert_rows(table_name, items)
            items = []

            end = timeit.default_timer()
            print 'processing time: {}'.format(end - start)

        #: delete seeded feature
        self.brick_layer.seed_features(arcpy, create=False)

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