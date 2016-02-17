#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''Crash dbseeder
Usage:
  dbseeder seed <source> <configuration>
  dbseeder create <configuration>
  dbseeder length <source>
  dbseeder (-h | --help | --version)
Options:
  -h --help     Show this screen.
'''


import sys
from dbseeder import DbSeeder
from docopt import docopt


def main():
    arguments = docopt(__doc__, version='1.0.2')

    seeder = DbSeeder()

    if arguments['length']:
        seeder.get_lengths(arguments['<source>'])
    elif arguments['create']:
        rel_path = ['data/sql/create_sql_tables.sql',
                    'data/sql/seed_spatial_data.sql']

        seeder.create_database(where=rel_path, who=arguments['<configuration>'])

        return 0
    elif arguments['seed']:
        seeder.process(arguments['<source>'], who=arguments['<configuration>'])

    return 0

if __name__ == '__main__':
    sys.exit(main())
