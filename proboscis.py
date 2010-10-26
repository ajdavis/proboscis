import sys
import time
import ConfigParser
from datetime import datetime
import argparse
import json
import pymongo

# Get options in 3 phases: first, set defaults, next, read proboscis.conf, finally,
# parse command-line options
host = 'localhost'
db_name = 'mongolog'
collection_name = 'log'
time_key = 'created'
message_key = 'msg'
timestamp_format = '%Y-%m-%d %H:%M:%S.%f:'

config = ConfigParser.ConfigParser()
config.read(['proboscis.conf'])

for section, option, varname in [
    ('mongodb', 'host', 'host'),
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

class StoreGlobal(argparse.Action):
    """
    Store an argument value as a global variable
    """
    def __call__(self, parser, namespace, value, option_string=None):
        globals()[self.dest] = value

parser = argparse.ArgumentParser()
parser.add_argument("-s", "--server", action=StoreGlobal, dest="host", default=host,
                    help="The server where mongodb resides")
parser.add_argument("filter_query", nargs="?", default="{}", help="Optional MongoDB query document")
args = parser.parse_args()

# Not configurable in proboscis.conf
filter_query = json.loads(args.filter_query)

db = pymongo.Connection(host)[db_name]

db[collection_name].ensure_index(time_key)
last_time = list(db[collection_name].find(filter_query, [time_key]).sort(time_key, pymongo.DESCENDING).limit(1))[0][time_key]

while True:
    query = {time_key: {'$gt': last_time}}
    query.update(filter_query)
    
    for row in db[collection_name].find(query).sort(time_key, pymongo.ASCENDING):
        message = row.get(message_key, None)
        timestamp = float(row[time_key])
        
        if message:
            print datetime.fromtimestamp(timestamp).strftime(timestamp_format),
            print message
        last_time = max(last_time, timestamp)
    
    time.sleep(1)
