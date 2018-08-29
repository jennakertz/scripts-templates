# GET BOOKMARKS

# this is the table path where you're saving bookmark for this script

# we recommend leaving this as-is
bookmark_schema = "_sdc_script_bookmarks"

# this is the unique name you used to create the script's bookmark table
# it is used construct the bookmark table name
script_name = "salesforce_dev"
bookmark_table_path = "{}._sdc_{}_bookmarks".format(bookmark_schema, script_name)

# this is a list of tables for which you need to get bookmark
# you may need to save bookmarks for multiple tables wihtin a single script
tables_in_script = "('table_1', 'table_2', 'table_3')"

# GET STORED BOOKMARKS
# grabs the max saved bookmark for each table
# and saves it in a variable called bookmarks

def get_bookmarks(bookmark_table_path, tables_in_script):
    bookmark_query = "select schema_name, table_name, max(bookmark) as bookmark from {} where table_name in {} group by 1,2".format(bookmark_table_path, tables_in_script)

    cur.execute(bookmark_query)
    results = cur.fetchall()

    bookmarks = results
    return bookmarks

def write_bookmark(bookmark_table_path, reference_schema, reference_table):
    # get new bookmark

    new_bookmark = "select max(_sdc_batched_at) as new_bookmark from {}.{}".format(reference_schema, reference_table)

    cur.execute(new_bookmark)
    new_bookmark = cur.fetchall()

    reference_table_bookmark_new = new_bookmark[0]['new_bookmark']

    # write new bookmark
    insert_bookmark = '''
    insert into {}(created_at, schema_name, table_name, bookmark) values (default,'{}','{}','{}')
    '''.format(bookmark_table_path, reference_schema, reference_table, reference_table_bookmark_new)

    cur.execute(insert_bookmark)
    conn.commit()

    return
    
try: 

    get_bookmarks(bookmark_table_path, tables_in_script)
    write_bookmark(bookmark_table_path, reference_table_path)    

    conn.close()
    
except Exception as e:
    
    logger.error('Closing connection due to error: ' + str(e))
    conn.close()
