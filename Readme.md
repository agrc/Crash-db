[![Build Status](https://travis-ci.org/agrc/Crash-db.svg?branch=master)](https://travis-ci.org/agrc/Crash-db)

Crash DB
========

A db seeder etl tool for crash data.

### Install
1. git clone this repo
1. cd into folder and run `./setup.py install`

### Usage
1. Get the csv's to use.
1. Create a `.sde` connection to the database you want to seed. Place them in `crashdb/connections`
1. Fill out `secrets.py`. Use the `secrets.sample.py` as an example.
1. run `python -m crashdb create <configuration>` where `<configuration>` is `dev, stage, prod` to create the database
1. run `python -m crashdb seed <source> <configuration>` where `<source>` is the path to the csv's `path/to/csv's` and `<configuration>` is `dev, stage, prod`. In dev, this is `crashdb\data\csv`

### Tests
`tox`

### Problems
`expecting string data` means the lookup value was not in the models table. Change batch size to 2 and look for a number where there should be a value. Add the number: None

`string or binary data to be truncated` - run `python -m crashdb path/to/csv's --length` and adjust sql schema

`ImportError: No module named x`. This means that x is not installed. Install the windows 64 bit python 2.7 module. If it is installed and you are in the tox environment you need to allow global site packages. For example update `C:\Users\agrc-arcgis\Envs\crash\Lib\no-global-site-packags.txt` to `allow-global-site-packages.txt`.

`_csv.Error: line contains NULL byte`. CSV's need to be resaved.

The points.json are out of sync with the map service. The etl will create new points.json. The [Crash-web](https://github.com/agrc/Crash-web) has a python script that get's run on deploys to sync also. **If these do not work**, check the rest queries. If they are returning `ESRI_OID` instead of `OBJECTED` then the map service query layer needs to be updated. Only check the `OBJECTID` field as unique. **Remove all other checks** and republish.

### Deployment
1. checkout repo
1. create `secrets.py`
1. make sure `pip` is installed
1. create `connections` folder within `src\crashdb`
1. run `pip install ./` from the current working directory containing `setup.py`
1. put connections and data folder inside `python\Lib\site-packages\crash_crashdb.egg\crashdb` if it's not already there

### I stink at doc's Development

1. With SQL Server create a `DDACTSadmin` and `DDACTSread` user
1. Run the `scripts\sql\create_db.sql` to create the `DDACTS` database. _You may need to modify paths based on sql server version/installation path_
1. User Map `DDACTSadmin` as a `db_owner` to `DDACTS` with the default schema of `DDACTSadmin`
1. Connect to the `DDACTS` db as `DDACTSadmin` and execute `src\crashdb\data\sql\create_sql_tables.sql`
1. Create CrashLocation fc
2. This should all work by `dbseeder create dev` but probably needs pro or something i was missing
