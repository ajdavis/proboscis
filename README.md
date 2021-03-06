Proboscis
=========

A simple command-line tail utility for mongodb. Requires pymongo.

Proboscis assumes you have a database called "mongolog" with a collection called "log". These values can be configured in proboscis.conf.

In the log collection, proboscis assumes your documents all have the following fields:

  * _created_: a float representing the unix epoch timestamp when the document was created. It's highly recommended that you index this field.
  * _msg_: a string of the log message

Any other fields in the document would represent metadata about the log message.

To run proboscis, simple call:

    python proboscis.py

This will start an infinite loop that polls mongolog.log every second for new documents, where new is determined by the "created" field. 

You can filter messages using mongodb's query language. For example:

    python proboscis.py "{'type': 'errors'}"
    
The argument should eval to a python dict. See [the MongoDB docs](http://www.mongodb.org/display/DOCS/Querying) for query syntax.

