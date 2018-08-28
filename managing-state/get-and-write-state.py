# GET STATE

# this is the table path where you're saving state for this script

# we recommend leaving this as-is
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

# GET NEW STATE

new_state = '''
select 
max(_sdc_batched_at) as new_state
from {}
'''.(reference_table_path)


try: 

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
