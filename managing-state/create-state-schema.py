# CREATE SCRIPT STATE SCHEMA

# user that should have access to the state
# same user that is connected in your stitch account
stitch_user = 'stitch_user'

# we recommend leaving this as-is
# it will be the schema your script states live in
state_schema = "_sdc_script_state"

# RUN ONCE - create state schema

# sql = '''
# create schema if not exists {} authorization {}
# '''.format(state_schema, stitch_user)

# cur.execute(sql)
# conn.commit()
# conn.close()
