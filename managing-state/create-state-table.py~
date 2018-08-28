# CREATE STATE TABLE FOR YOUR SCRIPT

# this is the schema your script states live in
# default is "_sdc_script_state"
state_schema = "_sdc_script_state"

# this is a unique name that will be used to create the script's state table
# it is used construct the state table name
script_name = "salesforce_dev"

state_table_path = "{}._sdc_{}_state".format(state_schema, script_name)

# RUN ONCE - create script's state table

# create_state_table = "create table if not exists {} (id bigint identity(1,1), created_at datetime default sysdate, table_name varchar, state bigint, primary key(id))".format(state_table_path)

# cur.execute(create_state_table)
# conn.commit()

# CHECK YOUR WORK :-)

# set search path to state schema
# set_path = 'set search_path to _sdc_script_state'
# cur.execute(set_path)

# verify the search path
# check_path = 'show search_path'
# cur.execute(check_path)
# cur.fetchall()

# verify the table structure

# check_structure = '''
# select "column", type
#  from pg_table_def where tablename = '_sdc_salesforce_dev_state';
# '''

# cur.execute(check_structure)
# results = cur.fetchall()

# results
# df = pd.DataFrame(results)
# df
