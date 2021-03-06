import logging
logger = logging.getLogger()

import pprint as pp
pp.pprint(connections)

import pandas as pd
from pandas import DataFrame
from collections import OrderedDict
from datetime import date

conn = connections['Default Warehouse']['client']
cur = conn.cursor()

# replace these variables

source_schema = 'chicken.trel'
source_table = 'trello_cards'
source_variant_field = 'badges'

dest_schema = 'chicken.burger'


try:
    
#     get all keys from the source variant field

#     note: you may specify optional arguments to the FLATTEN function to specify how the data is flattened
#     see: https://docs.snowflake.net/manuals/sql-reference/functions/flatten.html
    
    cur.execute("select distinct key from {}.{}, lateral flatten(input => parse_json({})) f;".format(source_schema,source_table,source_variant_field))
    result = cur.fetchall()
    
#     create a separate table for all keys included in the source variant field

#     definitions:
#         SEQ:    A unique sequence number associated with the input record; the sequence is not guaranteed to be gap-free or ordered in any particular way.
#         KEY:    For maps or objects, this column contains the key to the exploded value.
#         PATH:    The path to the element within a data structure which needs to be flattened.
#         INDEX:    The index of the element, if it is an array; otherwise NULL.
#         VALUE:    The value of the element of the flattened array/object.
#         THIS:    The element being flattened (useful in recursive flattening).
    
    for row in result:

        key = row[0]
        sql = cur.execute("create or replace table {}.{}__{} as select id as source_key_id, seq, path, index, value, this from chicken.trel.trello_cards, lateral flatten(input => parse_json(badges)) f where key = '{}';".format(dest_schema,source_table,key,key))
        
        logger.info('Successfully created {}.{}__{} table.'.format(dest_schema,source_table,key))
    
except Exception as e:
    logger.error(e)
finally:
    logger.info('Closing connection.')
    cur.close()
