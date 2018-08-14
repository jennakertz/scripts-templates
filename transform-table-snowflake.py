import logging
logger = logging.getLogger()

import pprint as pp
pp.pprint(connections)

# build transformation query

cnx = connections['Default Warehouse']['client']
cur = cnx.cursor()

try:
    sql = cur.execute("CREATE OR REPLACE TABLE chicken.burger.daily_14_agg AS SELECT count(*) as total FROM snowflake_sample_data.weather.daily_14_total")
    logger.info('Created aggregate table.')
except Exception as e:
    logger.error(e)
finally:
    logger.info('Closing connection.')
    cur.close()
