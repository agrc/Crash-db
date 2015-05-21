#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
services
----------------------------------
The basic services
'''

import datetime
import ceODBC as odbc
import os
from dateutil.parser import parse
from models import Schema


class Caster(object):

    """takes argis row input and casts it to the defined schema type"""
    @staticmethod
    def cast(value, cast_to):
        if value is None:
            return None

        try:
            value = value.strip()
        except:
            pass

        if cast_to == 'string':
            cast = str
        elif cast_to == 'int':
            cast = int
        elif (cast_to == 'float' or
              cast_to == 'double'):
            cast = float
        elif cast_to == 'date':
            if isinstance(value, datetime.datetime):
                cast = lambda x: x
            elif value == '':
                return None
            else:
                cast = parse
        elif cast_to == 'bool':
            cast = lambda x: x.lower() in ('yes', 'true', 't', '1')
        elif cast_to == 'bit':
            cast = Caster._cast_bit
        else:
            raise Exception(cast_to, 'No casting method created.')

        try:
            value = cast(value)

            if value == '':
                return None

            return value
        except:
            return None

    @staticmethod
    def _cast_bit(value):
        if value.lower() == 'y':
            return 1
        return 0


class BrickLayer(object):

    """inserts the records into the database"""

    def __init__(self, connection_string=None, creds=None):
        super(BrickLayer, self).__init__()

        self.batch_size = 10000
        self.arcpy_fields = {
            'crash': Schema.crash_fields
        }
        self.insert_statements = {
            'crash': 'INSERT INTO Crash VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            'rollup': 'INSERT INTO Rollup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            'driver': 'INSERT INTO Driver VALUES (?, ?, ?, ?, ?, ?, ?)'
        }
        self.connection_string = connection_string

        if not self.connection_string:
            script_dir = os.path.dirname(__file__)
            self.connection_string = (
                'DRIVER={SQL Server};' +
                'SERVER={};'.format(creds['server']) +
                'DATABASE=DDACTS;' +
                'UID={};'.format(creds['username']) +
                'PWD={};'.format(creds['password']))

            self.sde = os.path.join(script_dir,
                                    'connections',
                                    creds['sde_connection_path'])
            self.crash_table = os.path.join(script_dir,
                                            'connections',
                                            creds['sde_connection_path'],
                                            'DDACTS.DDACTSadmin.CrashLocation')

    def insert_rows_with_arcpy(self, table_name, rows):
        if table_name.lower() not in self.insert_statements.keys():
            raise Exception(table_name, 'Do not know how to insert this type of record')

        fields = self.arcpy_fields[table_name.lower()]

        print 'total rows to insert {}'.format(len(rows))
        from arcpy.da import InsertCursor
        with InsertCursor(self.crash_table, fields) as cursor:
            for row in rows:
                cursor.insertRow(row)

    def insert_rows(self, table_name, rows):
        if table_name.lower() not in self.insert_statements.keys():
            raise Exception(table_name, 'Do not know how to insert this type of record')

        if table_name == 'crash':
            return self.insert_rows_with_arcpy(table_name, rows)

        connection = odbc.connect(self.connection_string)
        cursor = connection.cursor()

        command = self.insert_statements[table_name.lower()]

        i = 1
        start = 0
        end = self.batch_size
        try:
            print 'total rows to insert {}'.format(len(rows))

            while start < len(rows):
                batched_rows = rows[start:end]

                cursor.executemany(command, batched_rows)
                connection.commit()

                i = i + 1
                start = end
                end = i * self.batch_size + 1
        except:
            # import pprint
            # pp = pprint.PrettyPrinter(indent=4)
            # pp.pprint(batched_rows)

            print '{} {}-{}'.format(table_name, start, end)

            raise
        finally:
            cursor.close()
            connection.close()

    def seed_features(self, arcpy, create=True):
        script_dir = os.path.dirname(__file__)

        if create:
            print('seeding spatial data')

            with open(os.path.join(script_dir, 'data/sql/seed_spatial_data.sql'), 'r') as f:
                sql = f.read()
        else:
            print('removing seeded data')

            with open(os.path.join(script_dir, 'data/sql/remove_seeded_data.sql'), 'r') as f:
                sql = f.read()

        try:
            c = arcpy.ArcSDESQLExecute(self.sde)
            c.execute(sql)
        except Exception, e:
            raise e
        finally:
            del c
