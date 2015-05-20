#!usr/bin/env python
# -*- coding: utf-8 -*-

'''
dbseeder
----------------------------------
test the dbseeder module
'''

import unittest
import datetime
from dbseeder.dbseeder import DbSeeder
from os.path import join
from os import sep
from nose.plugins.skip import SkipTest


class TestDbSeeder(unittest.TestCase):

    patient = None

    def setUp(self):
        self.patient = DbSeeder()
        self.maxDiff = None

    def test_process(self):
        raise SkipTest
        self.patient.process(join('.', 'tests', 'data'))

    def test_etl_rollup(self):
        row = {
            'BICYCLIST_INVOLVED': 'N',
            'COMMERCIAL_MOTOR_VEH_INVOLVED': 'Y',
            'CRASH_DATETIME': '2011-01-24 15:00:00',
            'CRASH_ID': '10380000',
            'DOMESTIC_ANIMAL_RELATED': 'N',
            'DUI': 'N',
            'IMPROPER_RESTRAINT': 'N',
            'INTERSECTION_RELATED': 'N',
            'MOTORCYCLE_INVOLVED': 'N',
            'NIGHT_DARK_CONDITION': 'N',
            'OLDER_DRIVER_INVOLVED': 'N',
            'OVERTURN_ROLLOVER': 'N',
            'PEDESTRIAN_INVOLVED': 'N',
            'TEENAGE_DRIVER_INVOLVED': 'N',
            'WILD_ANIMAL_RELATED': 'N'
        }
        expected = [
            # 'id':
            10380000,
            # 'date':
            datetime.datetime(2011, 1, 24, 15, 0, 0),
            # 'pedestrian':
            0,
            # 'bicycle':
            0,
            # 'motorcycle':
            0,
            # 'improper_restraint':
            0,
            # 'dui':
            0,
            # 'intersection':
            0,
            # 'animal_wild':
            0,
            # 'animal_domestic':
            0,
            # 'rollover':
            0,
            # 'commercial_vehicle':
            1,
            # 'teenager':
            0,
            # 'elder':
            0,
            # 'dark':
            0
        ]

        actual = self.patient._etl_row('rollup', row)
        self.assertEqual(actual, expected)

    def test_etl_crash(self):
        row = {
            'CASE_NUMBER': '11UT0004',
            'CITY': '',
            'COUNTY_NAME': 'UTAH',
            'CRASH_DATETIME': '2011-01-01 12:09:00',
            'CRASH_ID': '10376162',
            'CRASH_SEVERITY_ID': '1',
            'DAY': '7',
            'FIRST_HARMFUL_EVENT_ID': '59',
            'HOUR': '12',
            'MAIN_ROAD_NAME': 'SR 189',
            'MANNER_COLLISION_ID': '96',
            'MILEPOINT': '12.2',
            'MINUTE': '9',
            'MONTH': '1',
            'OFFICER_DEPARTMENT_CODE': 'UTUHP1000',
            'OFFICER_DEPARTMENT_NAME': 'UHPORE.UT.USA',
            'ROADWAY_SURF_CONDITION_ID': '5',
            'ROUTE_NUMBER': '189',
            'UTM_X': '450007',
            'UTM_Y': '4466748',
            'WEATHER_CONDITION_ID': '1',
            'WORK_ZONE_RELATED': 'N',
            'YEAR': '2011'
        }

        expected = [
            # 'id':
            10376162,
            # 'date':
            datetime.datetime(2011, 1, 1, 12, 9, 0),
            # 'year':
            2011,
            # 'month':
            1,
            # 'day':
            7,
            # 'hour':
            12,
            # 'minute':
            9,
            # 'construction':
            0,
            # 'weather_condition':
            'Clear',
            # 'road_condition':
            'Ice',
            # 'event':
            'Snow Bank',
            # 'collision_type':
            None,
            # 'severity':
            'No Injury/PDO',
            # 'case_number':
            '11UT0004',
            # 'officer_name':
            'UHPORE.UT.USA',
            # 'officer_department':
            'UTUHP1000',
            # 'road_name':
            'SR 189',
            # 'route_number':
            189,
            # 'milepost':
            12.2,
            # 'city':
            None,
            # 'county':
            'UTAH',
            # 'utm_x':
            450007.0,
            # 'utm_y':
            4466748.0
        ]

        actual = self.patient._etl_row('crash', row)
        self.assertEqual(actual, expected)

    def test_etl_driver(self):
        row = {
            'PEOPLE_DETAIL_ID': '11699638',
            'DRIVER_CONTRIB_CIRCUM_2_ID': '4',
            'DRIVER_DISTRACTION_ID': '0',
            'DRIVER_CONTRIB_CIRCUM_1_ID': '2',
            'CRASH_DATETIME': '2011-01-05 10:39:00',
            'VEHICLE_NUM': '1',
            'PERSON_ID': '1',
            'CRASH_ID': '10376425',
            'DRIVER_CONDITION_ID': '1'
        }

        expected = [
            # 'id':
            10376425,
            # 'date':
            datetime.datetime(2011, 1, 5, 10, 39, 0),
            # 'vehicle_count':
            1,
            # 'contributing_cause':
            'Too Fast for Conditions',
            # 'alternate_cause':
            'Failed to Keep in Proper Lane',
            # 'driver_condition':
            'Appearing Normal',
            # 'driver_distraction':
            'None'
        ]

        actual = self.patient._etl_row('driver', row)
        self.assertEqual(actual, expected)

    def test_etl_wrong_file_name(self):
        self.assertRaises(Exception, self.patient._etl_row, 'wrong')

    def test_get_files_without_trailing_slashes(self):
        actual = self.patient._get_files(join('.', 'tests', 'data'))

        self.assertEqual(len(actual), 3)

    def test_get_files_with_trailing_slashes(self):
        actual = self.patient._get_files('.{0}tests{0}data{0}'.format(sep))

        self.assertEqual(len(actual), 3)

    def test_get_files_empty_location_raises_exception(self):
        self.assertRaises(Exception, self.patient._get_files, '')

    def test_get_files_raises_if_empty(self):
        self.assertRaises(Exception, self.patient._get_files, [join('some', 'path', 'to', 'nowhere')])

    def test_get_table_name(self):
        actual = self.patient._get_table_name('balksdfj;lkasdf_driver')
        self.assertEqual(actual, 'driver')

        actual = self.patient._get_table_name('balksdfj;lkasdf_crash_234234')
        self.assertEqual(actual, 'crash')

        actual = self.patient._get_table_name('balksdfj;lkasdf_rollupasdf213')
        self.assertEqual(actual, 'rollup')

        actual = self.patient._get_table_name('balksdfj;lkasdf_rupasdf213')
        self.assertIsNone(actual)

    def test_get_lengths(self):
        actual = self.patient.get_lengths(join('.', 'tests', 'data'))

        expected = {
            'CASE_NUMBER': 92,
            'CITY': 11,
            'COUNTY_NAME': 9,
            'MAIN_ROAD_NAME': 11,
            'OFFICER_DEPARTMENT_CODE': 9,
            'OFFICER_DEPARTMENT_NAME': 13
        }

        self.assertEqual(actual['crash'], expected)
