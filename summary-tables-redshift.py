import psycopg2
import psycopg2.extras
import pprint as pp
import logging
import pandas

logger = logging.getLogger()

rs = connections['Default Warehouse']['client']
cur = rs.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# build command to create a new table
# use CREATE OR REPLACE VIEW name AS in order to avoid conflicts when trying to replace data
# https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_VIEW.html

create_table = '''
create or replace view build_summary_table as (

with a as (
select
cid as primary_key
from schema_a.table_a
limit 100
)
,

b as (
select 
client_id as foreign_key, 
count(*) as total
from schema_b.table_b
group by 1 
order by 2 desc
)
,

c as (
select
a.*,
nvl(b.total,0) as total
from a
left join b
on a.primary_key = b.foreign_key
)

select
total,
count(*)
from c
group by 1

);
'''

# execute command to create a new table
# log any exceptions as an error

try:
    cur.execute(create_table)
    logger.info('Successfully executed CREATE TABLE AS command')
except Exception as e: 
    logger.error(e)
    cur.close()

# make the changes to the database persistent
# log any exceptions as an error
# and close communication with the database

try:
    rs.commit()
    logger.info('Successfully committed changes to the database')
except Exception as e: 
    logger.error(e)
    cur.close()
    rs.close()

# verify the changes persisted
# log any exceptions as an error
# and close communication with the database


query = '''
select *
from build_summary_table
order by total_connections asc
'''

try:
    cur.execute(query)
#     resultdata = cur.fetchall()
#     pp.pprint(resultdata)
    logger.info('Successfully queried the newly created table from the database')
except Exception as e:
    logger.error(e)
    cur.close()
    rs.close()

# close communication with the database

cur.close()
rs.close()
