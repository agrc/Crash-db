#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
services
----------------------------------
test the service module
'''

import unittest
import datetime
import dbseeder.services as service


class TestCaster(unittest.TestCase):

    def test_raises_on_unknown_type(self):
        self.assertRaises(Exception, service.Caster.cast, ['abc', 'alphabet'])

    def test_empty_string_returns_none(self):
        actual = service.Caster.cast('', 'string')
        self.assertIsNone(actual, msg='text')

        actual = service.Caster.cast('', 'int')
        self.assertIsNone(actual, msg='int')

        actual = service.Caster.cast('', 'date')
        self.assertIsNone(actual, msg='date')

        actual = service.Caster.cast('', 'float')
        self.assertIsNone(actual, msg='float')

        actual = service.Caster.cast('', 'double')
        self.assertIsNone(actual, msg='double')

    def test_stripping_nonstring_is_ok(self):
        actual = service.Caster.cast(1, 'int')
        self.assertEqual(actual, 1, msg='int')

    def test_date_returns_itself(self):
        actual = service.Caster.cast(datetime.datetime(2011, 1, 5, 10, 39, 0), 'date')
        self.assertEqual(actual, datetime.datetime(2011, 1, 5, 10, 39, 0), msg='date')

    def test_none_always_returns_non(self):
        actual = service.Caster.cast(None, 'string')
        self.assertIsNone(actual, msg='text')

        actual = service.Caster.cast(None, 'int')
        self.assertIsNone(actual, msg='int')

        actual = service.Caster.cast(None, 'date')
        self.assertIsNone(actual, msg='date')

        actual = service.Caster.cast(None, 'float')
        self.assertIsNone(actual, msg='float')

        actual = service.Caster.cast(None, 'double')
        self.assertIsNone(actual, msg='double')

    def test_casts_values(self):
        actual = service.Caster.cast('abc', 'string')
        self.assertEqual(actual, 'abc', msg='text')

        actual = service.Caster.cast('1', 'int')
        self.assertEqual(actual, 1, msg='int')

        actual = service.Caster.cast('False', 'bool')
        self.assertEqual(actual, False)

        actual = service.Caster.cast('2011-01-05 10:39:00', 'date')
        self.assertEqual(actual, datetime.datetime(2011, 1, 5, 10, 39, 0), msg='date')

        actual = service.Caster.cast('1234567890.1234567890', 'float')
        self.assertEqual(actual, 1234567890.1234567890, msg='float')

        actual = service.Caster.cast('123.123', 'double')
        self.assertEqual(actual, 123.123, msg='double')

if __name__ == '__main__':
    unittest.main()
