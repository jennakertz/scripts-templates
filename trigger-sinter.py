# replace with your Sinter account details
# Sinter's API requires a paid account
# reach out to Sinter support for an API key

sinter_api_key = 'API-KEY'
sinter_account_id = 'ACCOUNT-ID'
sinter_project_id = 'PROJECT-ID'
sinter_job_def_id = 'JOB-DEF-ID'

headers = {'Authorization':'Token ' + sinter_api_key, 'Content-Type':'application/json'}
base_url = 'https://app.sinterdata.com/api/v1/accounts/'

# kick off a pre-defined Sinter job

r = requests.post(base_url + sinter_account_id + '/projects/' + sinter_project_id + '/definitions/' + sinter_job_def_id + '/runs/', headers=headers)
