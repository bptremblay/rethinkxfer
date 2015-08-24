The Problem
===========

No easy way to copy rethinkdb tables from one machine to another.

RethinkDB uses JSON natively, but we don’t have an easy way to dump all
data to JSON for versioning, seeding, and test purposes.

The Solution
============

This simple tool handles three data transfer points:

1.  The source server and database.

2.  Local JSON files.

3.  The destination server and database.

How it Works
============

We either copy data *FROM* a source server *TO* JSON files, or *FROM*
JSON files *TO* a destination server.

A configuration file (./rethinkxfer-config.json) is normally the easiest
way to declare the source and destination RethinkDB servers/databases,
as well as the default directionality.

{

"DUMP\_TO\_FILES": false, True if we wish to generate JSON from
rethinkdbCopy

"rethinkdbCreate": { Destination server/db.

"host": "localhost",

"port": 28015,

"db": "perfhub\_ben\_test",

"authKey": ""

},

"rethinkdbCopy": { Source server/db.

"host": "10.64.253.25",

"port": 8090,

"db": "QA\_DA\_TEST",

"authKey": ""

}

}

Running
=======

If you have not done so yet, use npm install to add the required node
modules.

The rethinkxfer tool runs from the command line, example:

node rethinkxfer –backup

or,

node rethinkxfer –restore

Output
======

The console will show what is being done, and runs asynchronously. When
everything has finished successfully, the console will report “ALL
DONE.”

Example:

\$ node rethinkxfer -backup

createJSONFromTables 0

createJSONFromTables: Create JSON from these tables:

\[ 'ApiSessions',

'associate',

'audit',

'campaign',

'category',

'clientlogs',

'interval',

'leaderboard',

'loadcontrol',

'manager',

'national',

'site',

'user' \]

createJSONFromTables: Creating "associate.json".

createJSONFromTables: Creating "ApiSessions.json".

createJSONFromTables: Creating "audit.json".

createJSONFromTables: Creating "clientlogs.json".

createJSONFromTables: Creating "campaign.json".

createJSONFromTables: Created "ApiSessions.json".

createJSONFromTables: Created "audit.json".

createJSONFromTables: Created "clientlogs.json".

createJSONFromTables: Created "campaign.json".

createJSONFromTables: Creating "category.json".

createJSONFromTables: Created "category.json".

createJSONFromTables: Creating "interval.json".

createJSONFromTables: Created "interval.json".

createJSONFromTables: Creating "leaderboard.json".

createJSONFromTables: Creating "manager.json".

createJSONFromTables: Creating "national.json".

createJSONFromTables: Creating "loadcontrol.json".

createJSONFromTables: Created "loadcontrol.json".

createJSONFromTables: Creating "site.json".

createJSONFromTables: Creating "user.json".

createJSONFromTables: Created "user.json".

createJSONFromTables: Created "national.json".

createJSONFromTables: Created "site.json".

createJSONFromTables: Created "manager.json".

createJSONFromTables: Created "leaderboard.json".

createJSONFromTables: Created "associate.json".

ALL DONE
