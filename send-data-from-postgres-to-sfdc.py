import psycopg2
import psycopg2.extras
import pprint as pp

import pandas as pd

import logging
logger = logging.getLogger()

conn = connections['Default Warehouse']['client']
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# GET BOOKMARKS

# this is the table path where you're saving bookmark for this script
# default is "_sdc_script_bookmarks"
bookmark_schema = "_sdc_script_bookmarks"

# this is the unique name you used to create the script's bookmark table
# it is used construct the bookmark table name
script_name = "salesforce_dev"
bookmark_table_path = "{}._sdc_{}_bookmarks".format(bookmark_schema, script_name)

# this is a list of tables for which you need to get bookmark
# you may need to save bookmarks for multiple tables wihtin a single script
tables_in_script = "('table_1', 'table_2', 'table_3')"

# GET STORED BOOKMARKS
# grabs the max saved bookmark for each table
# and saves it in a variable called bookmarks

def get_bookmarks(bookmark_table_path, tables_in_script):
    bookmark_query = "select schema_name, table_name, max(bookmark) as bookmark from {} where table_name in {} group by 1,2".format(bookmark_table_path, tables_in_script)

    cur.execute(bookmark_query)
    results = cur.fetchall()

    bookmarks = results
    return bookmarks


bookmarks = get_bookmarks(bookmark_table_path, tables_in_script)

# GET NEW DATA TO SEND TO SFDC

# in this example, we grab new signups from our website,
# and create contacts for them in salesforce

# this is the table we need to reference in our warehouse
reference_table = 'table_1'
reference_schema = 'schema'
reference_table_state = [x['bookmark'] for x in bookmarks if x['table_name'] == '{}'.format(reference_table) and x['schema_name'] = '{}'.format(reference_schema)]

get_data = '''
select
email, 
last_name
from {}.{}
where _sdc_batched_at > {}
'''.format(reference_schema, reference_table, reference_table_state)

def write_bookmark(bookmark_table_path, reference_schema, reference_table):
    # get new bookmark

    new_bookmark = "select max(_sdc_batched_at) as new_bookmark from {}.{}".format(reference_schema, reference_table)

    cur.execute(new_bookmark)
    new_bookmark = cur.fetchall()

    reference_table_bookmark_new = new_bookmark[0]['new_bookmark']

    # write new bookmark
    insert_bookmark = '''
    insert into {}(created_at, schema_name, table_name, bookmark) values (default,'{}','{}','{}')
    '''.format(bookmark_table_path, reference_schema, reference_table, reference_table_bookmark_new)

    cur.execute(insert_bookmark)
    conn.commit()

    return

try: 

    cur.execute(get_data)
    data = cur.fetchall()

    write_bookmark(bookmark_table_path, reference_schema, reference_table)
    
except Exception as e:
    
    logger.error('Closing connection due to error: ' + str(e))
    conn.close()

# CREATE NEW CONTACTS IN SFDC BASED ON THE DATA FROM THE WAREHOUSE

results = [sf.Contact.create({'Email': x['email'], 'LastName': x['last_name']}) for x in data]

# REPORT ON STATUS

from itertools import groupby
from operator import itemgetter

# sort and group results by status

results.sort(key=itemgetter('success'))
grouped_by_status = {str(key): list(group) for key, group in groupby(results, lambda el: el['success'])}


# `grouped_by_status` is  a dictionary keyed by the success property where the value for each entry is a 
# list of results with that status

success = grouped_by_status['True']

logger.info('Count of results by status:')
logger.info({k: len(v) for k, v in grouped_by_status.items()})
