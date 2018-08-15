import psycopg2
import psycopg2.extras
import pprint as pp
import logging
import pandas

logger = logging.getLogger()

conn = connections['Default Warehouse']['client']
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# replace these variables

schema = 'trello_ft_test'
table = 'trello_actions'
dist_key = 'date'
sort_key = 'id'
stitch_user = 'stitch_user'

# set search path
search_path = cur.execute("set search_path to {}".format(schema))

# get table definition

cur.execute("select a.column, a.type from pg_table_def a where schemaname = '{}' and tablename = '{}'".format(schema,table))
columns = ', '.join(['{} {}'.format(row['column'], row['type']) for row in cur])
        
# get table comment (primary key def)

sql = cur.execute("select description from pg_catalog.pg_description where objoid = (select oid from pg_class where relname = '{}' and relnamespace = (select oid from pg_catalog.pg_namespace where nspname = '{}'))".format(table,schema))

rec = cur.fetchone()

primary_key = rec['description']

try:
    alter_table = cur.execute("alter table {} rename to old_{}".format(table, table))
    create_table = cur.execute("create table new_{} ({}) distkey ({}) sortkey ({})".format(table, columns, dist_key, sort_key))
    insert = cur.execute("insert into new_{} (select * from old_{})".format(table, table))
    comment = cur.execute("comment on table new_{} is '{}'".format(table, primary_key))
    
    rename = cur.execute("alter table new_{} rename to {}".format(table, table))
    grant = cur.execute("alter table {} owner to {}".format(table, stitch_user))
    drop = cur.execute("drop table old_{}".format(table))
    
finally:
    cur.close()
    conn.close()
