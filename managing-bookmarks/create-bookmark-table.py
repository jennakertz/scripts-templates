# CREATE BOOKMARK TABLE FOR YOUR SCRIPT

# this is the schema your script states live in
# default is "_sdc_script_bookmarks"
bookmark_schema = "_sdc_script_bookmarks"

# this is a unique name that will be used to create the script's bookmark table
# it is used construct the bookmark table name
script_name = "salesforce_dev"

bookmark_table_path = "{}._sdc_{}_bookmarks".format(bookmark_schema, script_name)

# RUN ONCE - create script's bookmark table

# create_bookmark_table = "create table if not exists {} (id bigint identity(1,1), created_at datetime default sysdate, schema_name varchar, table_name varchar, bookmark datetime, primary key(id))".format(bookmark_table_path)

# cur.execute(create_bookmark_table)
# conn.commit()

# CHECK YOUR WORK :-)

# set search path to bookmark schema
set_path = 'set search_path to {}'.format(bookmark_schema)
cur.execute(set_path)

# verify the search path
check_path = 'show search_path'
cur.execute(check_path)
cur.fetchall()

# verify the table structure

check_structure = '''
select "column", type
from pg_table_def where tablename = '_sdc_{}_bookmarks';
'''.format(script_name)

cur.execute(check_structure)
results = cur.fetchall()

df = pd.DataFrame(results)
df


conn.close()
