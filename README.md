# Scripts Templates

Use the following templates to get started with Scripts.

| Template name | Destination type | Description |
| --- | --- | --- |
| [Add sort and dist keys](https://github.com/jennakertz/scripts-templates/blob/master/add-sort-dist-keys-redshift.py) | Redshift | Rebuilds an existing table with sort and dist key definitions, preserving table comments. While the table is rebuilt, copies existing data from the table into a temporary table. After the keys are defined on the new table, loads data from the temporary table back into the new table. Transfers ownership of the new table to the Stitch user. Drops temporary table. |
| [Flatten nested data](https://github.com/jennakertz/scripts-templates/blob/master/flatten-data-snowflake.py) | Snowflake | Flattens top-level elements of a VARIANT, OBJECT, or ARRAY field from a table into subtables. Creates new tables or replaces existing tables. | 
| [Create summary tables](https://github.com/jennakertz/scripts-templates/blob/master/summary-tables-redshift.py) | Redshift | Replaces or creates a new view in Redshift with a query built across tables. Can be run at regular intervals or after data loads in order to refresh data models. | 
| [Create a table snapshot](https://github.com/jennakertz/scripts-templates/blob/master/table-snapshot-bigquery.py) | BigQuery | Selects all of the data from a given source table and appends it to a destination table with the current timestamp. This deals with schema changes by appending columns that did not previously exist to the dataset. | 
| [Transform table](https://github.com/jennakertz/scripts-templates/blob/master/transform-table-snowflake.py) | Snowflake | Aggregate data from a given table into a new table using CREATE OR REPLACE TABLE AS. When this script is run, it will create a new table if one does not exist or replace the existing table. | 
| [Enrich data and store results](https://github.com/jennakertz/scripts-templates/blob/master/get-results-and-store-in-table-snowflake.py) | Snowflake | Query your data warehouse to get a list of data you'd like to enrich. Use that data to query an API like Clearbit. Store the results returned from the API in a table in your destination. |
| [Audit SFDC data's schema](https://github.com/jennakertz/scripts-templates/blob/master/audit-sfdc-fields-snowflake.py) | Snowflake | Compares an object's fields in SFDC to the columns for the object in the Snowflake data warehouse. If fields exist in SFDC but do not exist in the data warehouse, checks SFDC to see whether the missing fields contain data. If the missing fields contain data in SFDC, reports the problem fields along with record id examples for investigation. | 
| [Send data to SFDC](https://github.com/jennakertz/scripts-templates/blob/master/send-data-from-postgres-to-sfdc.py) | Postgres | Create new contacts in Salesforce when new data is loaded into your warehouse. | 


# Post-load scheduling

## Filtering tables

Scripts using post-load scheduling will recieve a variable called `event` when the Script is invoked. This variable contains the tables which were updated in your warehouse since the last time the script ran. If you want your Script to run for only specific tables, you can filter it like so:

```
import pprint as pp
import json

import logging
logger = logging.getLogger()

# dummy event variable
event = json.loads('{"script_id": 123,"tables": {"schema_a": ["table_a","table_b"],"schema_b": ["table_a"],"schema_c": ["table_a"]}}')

# RUN THIS SCRIPT ONLY FOR THE FOLLOWING SCHEMAS AND TABLES
# FILTER ANY ADDITIONAL TABLES FROM THE `event` VARIABLE OUT OF THE SCRIPT

accepted_tables = json.loads('{"schema_a":["table_b"], "schema_c":["table_c"]}')

a = event['tables']
b = accepted_tables

included_tables = {k: list(set(a[k]) & set(b[k])) for k in a if k in b}
pp.pprint(included_tables)

logger.info('Running Script with the following tables: ' + str(included_tables))
```

## Managing bookmarks

If you’re scheduling your Scripts based on new data that’s loaded into your warehouse, you may only want to process new or updated data from the tables in your Script. These templates will help you set up a system for keeping track of what data has already been processed by the Script.

| Name | Description |
| --- | --- |
| [Create a bookmark schema](https://github.com/jennakertz/scripts-templates/blob/master/managing-bookmarks/create-bookmark-schema.py) | We recommend creating a new schema in your data warehouse for your bookmark tracking tables. There will be a separate bookmark tracking table for each Script. | 
| [Create a bookmark table](https://github.com/jennakertz/scripts-templates/blob/master/managing-bookmarks/create-bookmark-table.py) | In your bookmark schema, create a table specifically for tracking bookmarks for the tables used in this Script. The bookmark table contains a unique id, a created_at date, the schema name for the reference table, the table name of the reference table, and the maximum bookmark value processed in the last run. |
| [Get and write bookmarks](https://github.com/jennakertz/scripts-templates/blob/master/managing-bookmarks/get-and-write-bookmarks.py) | Use these functions to retrieve bookmarks from your Script’s bookmark table and then write bookmarks back to that table. | 

