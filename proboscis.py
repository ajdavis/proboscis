import sys
import time
import ConfigParser
from datetime import datetime
import argparse
import json
import pymongo

# Get options in 3 phases: first, set defaults, next, read proboscis.conf, finally,
# parse command-line options
hosts = ['localhost']
db_name = 'mongolog'
collection_name = 'log'
time_key = 'created'
message_key = 'msg'
timestamp_format = '%Y-%m-%d %H:%M:%S.%f:'

config = ConfigParser.ConfigParser()
config.read(['proboscis.conf'])

for section, option, varname in [
    ('mongodb', 'db', 'db_name'),
    ('mongodb', 'collection', 'collection_name'),
    ('fields', 'time', 'time_key'),
    ('fields', 'message', 'message_key'),
    ('output', 'timestamp_format', 'timestamp_format'),
]:
    try:
        globals()[varname] = config.get(section, option)
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        pass

# Special parsing for 'host', which can be comma-delimited list
try:
    hosts = [i.strip() for i in config.get('mongodb', 'host').split(',')]
except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
    pass

class StoreGlobalList(argparse.Action):
    """
    Store an argument list as a global variable
    """
    def __call__(self, parser, namespace, value, option_string=None):
        globals()[self.dest] = [i.strip() for i in value.split(',')]

parser = argparse.ArgumentParser()
parser.add_argument("-s", "--server", action=StoreGlobalList, dest="hosts", default=hosts,
                    help="The server where mongodb resides")
parser.add_argument("filter_query", nargs="?", default="{}", help="Optional MongoDB query document")
args = parser.parse_args()

# Not configurable in proboscis.conf
filter_query = json.loads(args.filter_query)

dbs = [pymongo.Connection(host)[db_name] for host in hosts]

for db in dbs:
    db[collection_name].ensure_index(time_key)

last_times = [
    list(db[collection_name].find(filter_query, [time_key]).sort(time_key, pymongo.DESCENDING).limit(1))[0][time_key]
    for db in dbs
]

while True:
    for i, db in enumerate(dbs):
        query = {time_key: {'$gt': last_times[i]}}
        query.update(filter_query)
        
        for row in db[collection_name].find(query).sort(time_key, pymongo.ASCENDING):
            message = row.get(message_key, None)
            timestamp = float(row[time_key])
            
            if message:
                output = (datetime.fromtimestamp(timestamp).strftime(timestamp_format), message)
                if len(dbs) > 1:
                    output = (db.connection.host,) + output
                print ' '.join(output)
            last_times[i] = max(last_times[i], timestamp)
    
    time.sleep(1)
