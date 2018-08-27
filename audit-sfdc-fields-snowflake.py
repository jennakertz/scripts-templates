import logging
logger = logging.getLogger()

import requests
import json
import pprint as pp
import pandas as pd

from simple_salesforce import Salesforce

conn = connections['Default Warehouse']['client']
cur = conn.cursor()

sf = connections['salesforce_dev']['client']

# replace table path with relevant table path from your data warehouse

table_path = 'database.schema.table'

sql = cur.execute('select * from {}'.format(table_path)).fetchall()
df = pd.DataFrame(sql)
df

# get column names from your data warehouse

table_def = cur.execute('desc table {} type = columns'.format(table_path)).fetchall()
dw_field_names = [x[0] for x in table_def]

df = pd.DataFrame(column_names)
df

# replace "Opportunity" with relevant object name from SFDC

object_desc = sf.Opportunity.describe()['fields']

sf_field_names = [(x['name'].upper(), x['name']) for x in object_desc]

df = pd.DataFrame([x[0] for x in sf_field_names])
df

# create a list of fields that exist in SFDC but do not exist in the warehouse
# use the proper SFDC field name in the list

field_diff = list(set([x[0] for x in sf_field_names]) - set(dw_field_names))

field_diff_proper = [x[1] for x in sf_field_names if x[0] in set(field_diff)]

if len(field_diff_proper) > 0:
    logger.info('The following fields exist in SFDC but do not exist in your warehouse. Checking whether these fields contain data to replicate. ' + str(field_diff_proper))
    

# query SFDC to check whether there's data in any of the fields that are missing from the warehouse
# if data exists in SFDC for these fields, add them to a list called "misssing_field_report"

missing_field_report = []

for field in field_diff_proper: 
    
    # we can't use SOQL to filter on longform field types, like 'Description'
    # so we have to filter the results instead
    
    sf_query = "SELECT Id, {} FROM Opportunity".format(field)
    results = sf.query(sf_query)['records']
    
    if len([x[field] for x in results if x[field] is not None]) > 0:
        missing_field_report.append(field)
        records = []
        record_ids = [x['Id'] for x in results if x[field] is not None]
        records.append(record_ids)
        missing_field_report.append({'field':field, 'records':record_ids})
    else:
        logger.info('Field `{}` does not contain any data to replicate.'.format(field))

# log if there are field discrepancies

if len(missing_field_report) > 0:
    logger.info('The following fields have data in SFDC, but are missing from your warehouse. Here are the fields and example record Ids: ' + missing_field_report)
