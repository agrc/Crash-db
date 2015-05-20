#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
models
----------------------------------
The basic models
'''


class Schema(object):

    def __init__(self):
        super(Schema, self).__init__()

    driver = {
        'CRASH_ID': {
            'type': 'int',
            'map': 'crash_id'
        },
        'CRASH_DATETIME': {
            'type': 'date',
            'map': 'crash_date'
        },
        'VEHICLE_NUM': {
            'type': 'int',
            'map': 'vehicle_count'
        },
        'DRIVER_CONTRIB_CIRCUM_1_ID': {
            'type': 'int',
            'map': 'contributing_cause',
            'lookup': 'cause'
        },
        'DRIVER_CONTRIB_CIRCUM_2_ID': {
            'type': 'int',
            'map': 'alternate_cause',
            'lookup': 'cause'
        },
        'DRIVER_CONDITION_ID': {
            'type': 'int',
            'map': 'driver_condition',
            'lookup': 'driver_condition'
        },
        'DRIVER_DISTRACTION_ID': {
            'type': 'int',
            'map': 'driver_distraction',
            'lookup': 'driver_distraction'
        }
    }

    @staticmethod
    def driver_schema_ordering(d):
        return [
            d['crash_id'],
            d['crash_date'],
            d['vehicle_count'],
            d['contributing_cause'],
            d['alternate_cause'],
            d['driver_condition'],
            d['driver_distraction']
        ]

    driver_input_keys = driver.keys()
    driver_etl_keys = map(lambda x: x['map'], driver.values())

    rollup = {
        'BICYCLIST_INVOLVED': {
            'type': 'bit',
            'map': 'bicycle'
        },
        'COMMERCIAL_MOTOR_VEH_INVOLVED': {
            'type': 'bit',
            'map': 'commercial_vehicle'
        },
        'CRASH_DATETIME': {
            'type': 'date',
            'map': 'crash_date'
        },
        'CRASH_ID': {
            'type': 'int',
            'map': 'id'
        },
        'DOMESTIC_ANIMAL_RELATED': {
            'type': 'bit',
            'map': 'animal_domestic'
        },
        'DUI': {
            'type': 'bit',
            'map': 'dui'
        },
        'IMPROPER_RESTRAINT': {
            'type': 'bit',
            'map': 'improper_restraint'
        },
        'INTERSECTION_RELATED': {
            'type': 'bit',
            'map': 'intersection'
        },
        'MOTORCYCLE_INVOLVED': {
            'type': 'bit',
            'map': 'motorcycle'
        },
        'NIGHT_DARK_CONDITION': {
            'type': 'bit',
            'map': 'dark'
        },
        'OLDER_DRIVER_INVOLVED': {
            'type': 'bit',
            'map': 'elder'
        },
        'OVERTURN_ROLLOVER': {
            'type': 'bit',
            'map': 'rollover'
        },
        'PEDESTRIAN_INVOLVED': {
            'type': 'bit',
            'map': 'pedestrian'
        },
        'TEENAGE_DRIVER_INVOLVED': {
            'type': 'bit',
            'map': 'teenager'
        },
        'WILD_ANIMAL_RELATED': {
            'type': 'bit',
            'map': 'animal_wild'
        }
    }

    @staticmethod
    def rollup_schema_ordering(d):
        return [
            d['id'],
            d['crash_date'],
            d['pedestrian'],
            d['bicycle'],
            d['motorcycle'],
            d['improper_restraint'],
            d['dui'],
            d['intersection'],
            d['animal_wild'],
            d['animal_domestic'],
            d['rollover'],
            d['commercial_vehicle'],
            d['teenager'],
            d['elder'],
            d['dark']
        ]

    rollup_input_keys = rollup.keys()
    rollup_etl_keys = map(lambda x: x['map'], rollup.values())

    crash = {
        'CASE_NUMBER': {
            'type': 'string',
            'map': 'case_number'
        },
        'CITY': {
            'type': 'string',
            'map': 'city'
        },
        'COUNTY_NAME': {
            'type': 'string',
            'map': 'county'
        },
        'CRASH_DATETIME': {
            'type': 'date',
            'map': 'crash_date'
        },
        'CRASH_ID': {
            'type': 'int',
            'map': 'crash_id'
        },
        'CRASH_SEVERITY_ID': {
            'type': 'int',
            'map': 'severity',
            'lookup': 'severity'
        },
        'DAY': {
            'type': 'int',
            'map': 'crash_day'
        },
        'FIRST_HARMFUL_EVENT_ID': {
            'type': 'int',
            'map': 'event',
            'lookup': 'event'
        },
        'HOUR': {
            'type': 'int',
            'map': 'crash_hour'
        },
        'MAIN_ROAD_NAME': {
            'type': 'string',
            'map': 'road_name'
        },
        'MANNER_COLLISION_ID': {
            'type': 'int',
            'map': 'collision_type',
            'lookup': 'collision_type'
        },
        'MILEPOINT': {
            'type': 'float',
            'map': 'milepost'
        },
        'MINUTE': {
            'type': 'int',
            'map': 'crash_minute'
        },
        'MONTH': {
            'type': 'int',
            'map': 'crash_month'
        },
        'OFFICER_DEPARTMENT_CODE': {
            'type': 'string',
            'map': 'officer_department'
        },
        'OFFICER_DEPARTMENT_NAME': {
            'type': 'string',
            'map': 'officer_name'
        },
        'ROADWAY_SURF_CONDITION_ID': {
            'type': 'int',
            'map': 'road_condition',
            'lookup': 'road_condition'
        },
        'ROUTE_NUMBER': {
            'type': 'int',
            'map': 'route_number'
        },
        'UTM_X': {
            'type': 'float',
            'map': 'utm_x'
        },
        'UTM_Y': {
            'type': 'float',
            'map': 'utm_y'
        },
        'WEATHER_CONDITION_ID': {
            'type': 'int',
            'map': 'weather_condition',
            'lookup': 'weather_condition'
        },
        'WORK_ZONE_RELATED': {
            'type': 'bit',
            'map': 'construction'
        },
        'YEAR': {
            'type': 'int',
            'map': 'crash_year'
        }
    }

    @staticmethod
    def crash_schema_ordering(d):
        geometry = (d['utm_x'], d['utm_y'])
        return [
            geometry,
            d['crash_id'],
            d['crash_date'],
            d['crash_year'],
            d['crash_month'],
            d['crash_day'],
            d['crash_hour'],
            d['crash_minute'],
            d['construction'],
            d['weather_condition'],
            d['road_condition'],
            d['event'],
            d['collision_type'],
            d['severity'],
            d['case_number'],
            d['officer_name'],
            d['officer_department'],
            d['road_name'],
            d['route_number'],
            d['milepost'],
            d['city'],
            d['county'],
            d['utm_x'],
            d['utm_y']
        ]

    crash_fields = ['shape@',
                    'crash_id',
                    'crash_date',
                    'crash_year',
                    'crash_month',
                    'crash_day',
                    'crash_hour',
                    'crash_minute',
                    'construction',
                    'weather_condition',
                    'road_condition',
                    'event',
                    'collision_type',
                    'severity',
                    'case_number',
                    'officer_name',
                    'officer_department',
                    'road_name',
                    'route_number',
                    'milepost',
                    'city',
                    'county',
                    'utm_x',
                    'utm_y']
    crash_input_keys = crash.keys()
    crash_etl_keys = map(lambda x: x['map'], crash.values())


class Lookup(object):

    def __init__(self):
        super(Lookup, self).__init__()

    construction = {
        'Y': 1,
        'N': 0,
        'U': None
    }

    weather_condition = {
        1: 'Clear',
        2: 'Cloudy',
        3: 'Rain',
        4: 'Snowing',
        5: 'Blowing Snow',
        6: 'Sleet, Hail',
        7: 'Fog, Smog',
        8: 'Severe Crosswinds',
        9: 'Blowing Sand, Soil, Dirt',
        88: None,
        89: None,
        96: None,
        99: None
    }

    road_condition = {
        1: 'Dry',
        2: 'Wet',
        3: 'Snow',
        4: 'Slush',
        5: 'Ice',
        6: 'Water (standing,moving)',
        7: 'Mud',
        8: 'Sand, Dirt, Gravel',
        9: 'Oil',
        88: None,
        89: None,
        96: None,
        97: None,
        99: None
    }

    event = {
        0: 'No Damage or Injury, This Vehicle',
        1: 'Ran Off Road Right',
        2: 'Ran Off Road Left',
        3: 'Crossed Median/Centerline',
        4: 'Equipment Failure (tire, brakes, etc.)',
        5: 'Separation of Units',
        6: 'Downhill Runaway',
        7: 'Overturn/Rollover',
        8: 'Cargo/Equipment Loss or Shift',
        9: 'Jackknife',
        10: 'Fire/Explosion',
        11: 'Immersion',
        12: 'Fell/Jumped From Motor Vehicle',
        19: 'Other Non-Collision*',
        20: 'Other Motor Vehicle in Transport',
        21: 'Parked Motor Vehicle (off roadway)',
        22: 'Pedestrian',
        23: 'Pedalcycle',
        24: 'Skates, Scooters, Skateboards',
        25: 'Animal - Wild',
        26: 'Animal - Domestic',
        27: 'Work Zone/Maintenance Equipment',
        28: 'Freight Rail',
        29: 'Light Rail',
        30: 'Passenger Heavy Rail',
        31: 'Thrown or Fallen Object',
        32: None,
        33: None,
        34: None,
        35: None,
        36: None,
        37: None,
        38: None,
        39: 'Other Non-Fixed Object*',
        40: 'Guardrail',
        41: 'Concrete Barrier',
        42: 'Cable Barrier',
        43: 'Crash Cushion',
        44: 'Guardrail End Section',
        45: 'Concrete Sloped End Section',
        46: 'Cable Barrier End Section',
        47: 'Access Control Cable',
        48: 'Bridge Rail',
        49: 'Bridge Pier or Support',
        50: 'Bridge Overhead Structure',
        51: 'Traffic Sign Support',
        52: 'Delineator Post',
        53: 'Other Post, Pole or Support',
        54: 'Utility Pole/Light Support',
        55: 'Traffic Signal Support',
        56: 'Culvert',
        57: 'Ditch',
        58: 'Embankment',
        59: 'Snow Bank',
        60: 'Tree/Shrubbery',
        61: 'Mailbox/Fire Hydrant',
        62: 'Fence',
        63: 'Curb',
        69: 'Other Fixed Object',
        88: None,
        89: None,
        96: None,
        99: None
    }

    collision_type = {
        1: 'Angle',
        2: 'Front to Rear',
        3: 'Head On (front-to-front)',
        4: 'Sideswipe Same Direction',
        5: 'Sideswipe Opposite Direction',
        6: 'Parked Vehicle',
        7: 'Rear to Side',
        8: 'Rear to Rear',
        88: None,
        89: None,
        96: None,
        97: None,
        99: None
    }

    severity = {
        1: 'None',
        2: 'Non-Fatal',
        3: 'Non-Fatal',
        4: 'Non-Fatal',
        5: 'Fatal',
        88: None,
        89: None
    }

    cause = {
        0: 'None',
        1: 'Exceeded Posted Speed Limit',
        2: 'Too Fast for Conditions',
        3: 'Failed to Yield Right-of-Way',
        4: 'Failed to Keep in Proper Lane',
        5: 'Improper Lane Change',
        6: 'Over-Correcting/Over-Steering',
        7: 'Disregard Traffic Signs',
        8: 'Disregard Traffic Signals',
        9: 'Disregard Road Markings',
        10: 'Swerved or Evassive Action',
        11: 'Followed Too Closely',
        12: 'Reckless/Aggressive',
        13: 'Wrong Side/Wrong Way',
        14: 'Improper Parking/Stopping',
        15: 'Ran Off Road',
        16: 'Improper Backing',
        17: 'Improper Signal',
        18: 'Improper Passing',
        19: 'Improper Turn',
        20: 'Hit and Run',
        21: 'Street Racing',
        88: None,
        89: None,
        96: None,
        97: None,
        99: None
    }

    driver_condition = {
        1: 'Appearing Normal',
        2: 'Illness / Medical',
        3: 'Fatigue/Asleep',
        4: 'Exceeded HOS Limits',
        5: 'Under the Influence of Alcohol/Drugs/Medications',
        6: 'Emotional / Prior to',
        88: None,
        89: None,
        96: None,
        97: None,
        99: None
    }

    driver_distraction = {
        0: 'None',
        1: 'Cell Phone',
        2: 'Radio / CD / DVD etc.',
        3: 'Other Electronic Device',
        4: 'Passengers',
        5: 'Texting',
        6: 'TV / Monitor',
        7: 'Other Inside',
        8: 'Other External',
        88: None,
        89: None,
        96: None,
        97: None,
        99: None
    }
