import pandas

import pprint as pp
pp.pprint(connections)

import logging
logger = logging.getLogger()

from google.cloud import bigquery
client = connections['Default Warehouse']['client']

# # DO ONLY ONCE
# # create a new dataset for your daily snapshots

# dataset_ref = client.dataset('snapshots_dataset')

# dataset = bigquery.Dataset(dataset_ref)
# dataset.location = 'US'
# client.create_dataset(dataset)

# # DO ONLY ONCE
# # create a new table

# table_ref = dataset_ref.table('shakespeare_daily')
# table = bigquery.Table(table_ref)
# table = client.create_table(table)  # API request

# assert table.table_id == 'shakespeare_daily'

# get info on destination dataset

dataset_ref = client.dataset('snapshots_dataset')

# Retrieves the destination table and checks the length of the schema
table_id = 'shakespeare_daily'
table_ref = dataset_ref.table(table_id)
table = client.get_table(table_ref)

print("Table {} contains {} columns.".format(table_id, len(table.schema)))
logger.info("Table {} contains {} columns.".format(table_id, len(table.schema)))

# configure the query to append the results to a destination table,
# allowing field addition

try:
    job_config = bigquery.QueryJobConfig()
    job_config.schema_update_options = [
        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
    ]
    job_config.destination = table_ref
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
except Exception as e:
    logger.error(e)

query_job = client.query(
    
#     select everything from your source dataset
#     and add a created_at column with the current timestamp
    'SELECT *, CURRENT_DATETIME() as created_at from `bigquery-public-data.samples.shakespeare`;', # current_datetime returns UTC timestamp
    
# Location must match that of the dataset(s) referenced in the query
# and of the destination table.
    location='US',
    job_config=job_config

)  

try:
    query_job.result()  # Waits for the query to finish
    print("Query job {} complete.".format(query_job.job_id))
    logger.info("Query job {} complete.".format(query_job.job_id))
except Exception as e:
    logger.error(e)

# check the updated length of the schema

table = client.get_table(table)

print("Table {} now contains {} columns.".format(table_id, len(table.schema)))
logger.info("Table {} now contains {} columns.".format(table_id, len(table.schema)))

# look at the new timestamps

sql = """
    select distinct created_at
    from snapshots_dataset.shakespeare_daily
    limit 10
"""

client.query(sql).to_dataframe()
