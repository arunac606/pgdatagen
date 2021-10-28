import random
import string
import datetime
from numpy import random as nrandom
from numpy import unique as unique
from configparser import ConfigParser

def getschema(filename='database.ini', section='schema'):
    parser = ConfigParser()
    parser.read(filename)
    return parser

def getrange(filename='database.ini', section='range'):
    parser = ConfigParser()
    parser.read(filename)
    return parser

def gen_date():
    params=getrange()
    datestring = datetime.datetime.strptime(datetime.datetime(\
        random.randint(int(params['range']['minyear']),int(params['range']['maxyear'])),\
        random.randint(1, 12),\
        random.randint(1, 28),\
        random.randrange(23),\
        random.randrange(59),\
        random.randrange(59),\
        random.randrange(1000000)), '%Y-%m-%d %H:%M:%s')
    return datestring

def autogen_value(row):
    params = getrange()
    if row.data_type == 'integer':
        value = random.randint(int(params['range']['mininteger']),int(params['range']['maxinteger']))
    elif row.data_type == 'bigint':
        value = random.randint(int(params['range']['minbigint']),int(params['range']['maxbigint']))
    elif row.data_type == 'character varying'\
      or row.data_type == 'text':
        value = ''.join(random.choices(string.ascii_uppercase+string.digits,k=row.maximum_length))
    elif row.data_type == 'date':
        value = datetime.date(random.randint(int(params['range']['minyear']),int(params['range']['maxyear'])),\
                              random.randint(1, 12), random.randint(1, 28))
    elif row.data_type == 'timestamp without time zone':
        value = gen_date()
    elif row.data_type == 'bytea':
        value = nrandom.bytes(row.maximum_length)
    return value