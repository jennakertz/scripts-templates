import logging
logger = logging.getLogger()

import requests
import json
import pprint as pp

conn = connections['Default Warehouse']['client']
cur = conn.cursor()

# replace query definition

query = '''
select 'jenna@stitchdata.com'
union all
select 'fakepersonnotrealemail@yahoo.com'
union all
select 'jennakertz@gmail.com'
'''

# rename list "emails" to your list name

emails = []

cur.execute(query)
results = cur.fetchall()

for row in results:
    emails.append(row[0])

print(emails)

# information about the API you want to query

headers = {'Authorization':'Bearer API-KEY', 'Content-Type':'application/json'}
url = 'https://person.clearbit.com/v2/people/find'
search_param = '?email='

results = []

# query the api and store the results in a variable called results

for email in emails:
    try:
        r = requests.get(url + search_param + email, headers=headers)
        results.append({'status':r.status_code, 'email': email, 'response':json.loads(r.text)})
    except Exception as e:
        continue

pp.pprint(results)

# separate the results set so you can identify which queries were successful. you may want to retry certain requests.

from itertools import groupby
from operator import itemgetter

# sort and group results by status

results.sort(key=itemgetter('status'))
grouped_by_status = {str(key): list(group) for key, group in groupby(results, lambda el: el['status'])}


# `grouped_by_status` is  a dictionary keyed by status code where the value for each entry is a 
# list of results with that status

success = grouped_by_status['200']

logger.info('Count of results by status:')
logger.info({k: len(v) for k, v in grouped_by_status.items()})

# format the successful results for insertion into the data table

field_values = ["('{}', '{}', '{}', '{}', '{}', '{}')".format(element['email'], element['response']['name']['givenName'], element['response']['name']['familyName'], element['response']['employment']['name'], element['response']['employment']['title'], element['response']['gender']) for element in success]

# define the path of the table you want to create

table_path = 'database.schema.table'

# run ONCE

# sql = cur.execute('CREATE OR REPLACE TABLE {} (email varchar, first_name varchar, last_name varchar, company varchar, title varchar, gender varchar)'.format(table_path))

# insert fields into table

try:
    cur.execute("INSERT INTO {} VALUES {}".format(table_path, ', '.join(str(x) for x in field_values)))
finally:
    logger.info('Closing connection.')
    cur.close()
