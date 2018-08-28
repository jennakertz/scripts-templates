import psycopg2
import psycopg2.extras
import pprint as pp

import pandas as pd

import logging
logger = logging.getLogger()

conn = connections['Default Warehouse']['client']
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# GET STATE

# this is the table path where you're saving state for this script
# the default is "_sdc_script_state"
state_schema = "_sdc_script_state"

# this is the unique name you used to create the script's state table
# it is used construct the state table name
script_name = "salesforce_dev"
state_table_path = "{}._sdc_{}_state".format(state_schema, script_name)

# this is a list of tables for which you need to get state
# you may need to save state for multiple tables
table_states = '(table_1, table_2, table_3)'

# grabs the max saved state for each table
# and saves it in a variable called state

state_query = '''
select
table,
max(state) as state
from
{}
where table_name in {}
group by 1
'''.format(state_table_path, table_states)

cur.execute(state_query)
results = cur.fetchall()

state = results

# GET NEW DATA TO SEND TO SFDC

# in this example, we grab new signups from our website,
# and create contacts for them in salesforce

# this is the table we need to reference in our warehouse
reference_table_path = 'table_1'
reference_table_state = [x['state'] for x in state if x['table'] == '{}'.format(reference_table_path)]

get_data = '''
select
email, 
last_name
from {}
where _sdc_batched_at > {}
'''.format(reference_table_path, reference_table_state)

# RETRIEVE NEW STATE

new_state = '''
select 
max(_sdc_batched_at) as new_state
from {}
'''.(reference_table_path)

try: 
    cur.execute(get_data)
    data = cur.fetchall()

    cur.execute(new_state)
    new_state = cur.fetchall()

    reference_table_state_new = new_state[0]['new_state']
    
    # write new state
    insert_state = '''
    insert into {}(created_at, table_name, state) values (default,'{}',{})
    '''.format(state_table_path, reference_table_path, reference_table_state_new)

    cur.execute(insert_state)
    
    conn.commit()
    
except Exception as e:
    
    logger.error('Closing connection due to error: ' + str(e))
    conn.close()
    
pd.DataFrame(data)

# CREATE NEW CONTACTS IN SFDC BASED ON THE DATA FROM THE WAREHOUSE

results = [sf.Contact.create({'Email': x['email'], 'LastName': x['last_name']}) for x in data]

# REPORT ON STATUS

from itertools import groupby
from operator import itemgetter

# sort and group results by status

results.sort(key=itemgetter('success'))
grouped_by_status = {str(key): list(group) for key, group in groupby(results, lambda el: el['success'])}


# `grouped_by_status` is  a dictionary keyed by status code where the value for each entry is a 
# list of results with that status

success = grouped_by_status['True']

logger.info('Count of results by status:')
logger.info({k: len(v) for k, v in grouped_by_status.items()})
