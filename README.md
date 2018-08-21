# Scripts Templates

Use the following templates to get started with Scripts.

| Template name | Destination type | Description |
| --- | --- | --- |
| [`add-sort-dist-keys-redshift.py`](https://github.com/jennakertz/scripts-templates/blob/master/add-sort-dist-keys-redshift.py) | Redshift | Rebuilds an existing table with sort and dist key definitions, preserving table comments. While the table is rebuilt, copies existing data from the table into a temporary table. After the keys are defined on the new table, loads data from the temporary table back into the new table. Transfers ownership of the new table to the Stitch user. Drops temporary table. |
| [flatten-data-snowflake.py](https://github.com/jennakertz/scripts-templates/blob/master/flatten-data-snowflake.py) | Snowflake | Flattens top-level elements of a VARIANT, OBJECT, or ARRAY field from a table into subtables. Creates new tables or replaces existing tables. | 
| [summary-tables-redshift.py](https://github.com/jennakertz/scripts-templates/blob/master/summary-tables-redshift.py) | Redshift | Replaces or creates a new view in Redshift with a query built across tables. Can be run at regular intervals or after data loads in order to refresh data models. | 
| [table-snapshot-bigquery.py](https://github.com/jennakertz/scripts-templates/blob/master/table-snapshot-bigquery.py) | BigQuery | Selects all of the data from a given source table and appends it to a destination table with the current timestamp. This deals with schema changes by appending columns that did not previously exist to the dataset. | 
| [transform-table-snowflake.py](https://github.com/jennakertz/scripts-templates/blob/master/transform-table-snowflake.py) | Snowflake | Aggregate data from a given table into a new table using CREATE OR REPLACE TABLE AS. When this script is run, it will create a new table if one does not exist or replace the existing table. | 
