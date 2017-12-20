#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''Crash ETL CLI
Usage:
  crashdb seed <source> <configuration> [--testing]
  crashdb create <configuration> [--testing]
  crashdb length <source> [--testing]
  crashdb (-h | --help | --version)
Options:
  -h --help     Show this screen.
  <configuration> dev, stage, prod
'''


import sys

from docopt import docopt

from . import secrets
from .crashseeder import CrashSeeder
from .logger import Logger
from .mailman import MailMan


def main():
    arguments = docopt(__doc__, version='2.1.0')
    testing = arguments['--testing']

    mailman = MailMan('sgourley@utah.gov', testing=testing)
    logger = Logger(script_name='crash-db', stdout=testing)
    seeder = CrashSeeder(logger)

    def global_exception_handler(ex_cls, ex, tb):
        import traceback

        last_traceback = (traceback.extract_tb(tb))[-1]
        line_number = last_traceback[1]
        file_name = last_traceback[0].split(".")[0]

        logger.error(traceback.format_exception(ex_cls, ex, tb))

        mailman.deliver('crash-db error', logger.print_log())

        if testing:
            traceback.print_exception(ex_cls, ex, tb)

            logger.info(logger.print_log(), stdout=testing)

            logger.info(("line:%s (%s)" % (line_number, file_name)), stdout=testing)

    sys.excepthook = global_exception_handler

    if arguments['length']:
        seeder.get_lengths(arguments['<source>'])
    elif arguments['create']:
        rel_path = ['data/sql/create_sql_tables.sql',
                    'data/sql/seed_spatial_data.sql']

        seeder.create_database(where=rel_path, who=arguments['<configuration>'])
    elif arguments['seed']:
        seeder.process(arguments['<source>'], who=arguments['<configuration>'])

    if testing:
        print(logger.print_log())
    else:
        creds = secrets.dev
        if arguments['<configuration>'] == 'stage':
            creds = secrets.stage
        elif arguments['<configuration>'] == 'prod':
            creds = secrets.prod

        logger.save(creds['logs'])

    return 0
