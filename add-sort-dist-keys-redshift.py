import psycopg2
import psycopg2.extras
import pprint as pp
import logging
import pandas

logger = logging.getLogger()

rs = connections['Default Warehouse']['client']
cur = rs.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# replace these variables

schema = 'trello_ft_test'
table = 'trello_actions'
dist_key = 'date'
sort_key = 'id'
stitch_user = 'stitch_user'

# set search path
search_path = cur.execute("set search_path to %s" %schema)

# get table definition
sql = cur.execute("select a.column, a.type from pg_table_def a where schemaname = '%s' and tablename = '%s'" % (schema,table))
result = cur.fetchall()

# print(len(result))
i = 0
columns = ''

for row in result:
    i = i + 1
    
    if i == len(result):
        columns = columns + ' ' + row['column'] + ' ' + row['type']
    else:
        columns = columns + ' ' + row['column'] + ' ' + row['type'] + ','
        
# get table comment (primary key def)

sql = cur.execute("select description from pg_catalog.pg_description where objoid = (select oid from pg_class where relname = '%s' and relnamespace = (select oid from pg_catalog.pg_namespace where nspname = '%s'))"%(table,schema))

rec = cur.fetchone()

primary_key = rec['description']

try:
    alter_table = cur.execute("alter table %s rename to old_%s" %(table, table))
    create_table = cur.execute("create table new_%s (%s) distkey (%s) sortkey (%s)" %(table, columns, dist_key, sort_key))
    insert = cur.execute("insert into new_%s (select * from old_%s)" %(table, table))
    comment = cur.execute("comment on table new_%s is '%s'" %(table, primary_key))
    
    rename = cur.execute("alter table new_%s rename to %s" %(table, table))
    grant = cur.execute("alter table %s owner to %s" %(table, stitch_user))
    drop = cur.execute("drop table old_%s" %(table))

finally:
    cur.close()
    rs.close()
