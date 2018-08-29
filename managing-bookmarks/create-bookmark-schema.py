# CREATE SCRIPT BOOKMARK SCHEMA

# user that should have access to the bookmark
# same user that is connected in your stitch account
stitch_user = 'stitch_user'

# we recommend leaving this as-is
# it will be the schema your script bookmarks live in
bookmark_schema = "_sdc_script_bookmarks"

# RUN ONCE - create bookmark schema

# sql = '''
# create schema if not exists {} authorization {}
# '''.format(bookmark_schema, stitch_user)

# cur.execute(sql)
# conn.commit()
# conn.close()
