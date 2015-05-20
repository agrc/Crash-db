[![Build Status](https://travis-ci.org/agrc/Crash-db.svg?branch=travis)](https://travis-ci.org/agrc/Crash-db)

Crash DB
========

A db seeder etl tool for crash data.

### Usage
1. Get the csv's to use.
1. Create a .sde connection to the database you want to seed. Place them in dbseeder/connections
1. Fill out secrets.py. Use the secrets.sample.py as an example.
1. Update the __main__.py line 31 to be dev stage or prod.
1. run `python -m dbseeder create <configuration>` where `<configuration>` is `dev, stage, prod` to create the database
1. run `python -m dbseeder seed <source> <configuration` where `<source>` is the path to the csv's `path/to/csv's` and `<configuration>` is `dev, stage, prod`

### Tests
`tox`

### Problems
`expecting string data` means the lookup value was not in the models table. Change batch size to 2 and look for a number where there should be a value. Add the number: None

`string or binary data to be truncated` - run `python -m dbseeder path/to/csv's --length` and adjust sql schema 

`ImportError: No module named x`. This means that x is not installed. Install the windows 64 bit python 2.7 module. If is installed and you are in the tox environment you need to allow global site packages. For example update `C:\Users\agrc-arcgis\Envs\crash\Lib\no-global-site-packags.txt` to `allow-global-site-packages.txt`. ceODBC is not in pip.

`_csv.Error: line contains NULL byte`. CSV's need to be resaved.
