import ncl_sqlsnippets as snips
import time
import pyodbc

#Force delay between requests
def add_delay(seconds):
    time.sleep(int(seconds))

# Sandpit data pull

def get_sandpit_data(env, query):
    engine = snips.connect(env["SQL_ADDRESS"], env["SQL_DATABASE"])
    res = snips.execute_sfw(engine, query)
    return res

def get_full_table_name(env):

    sql_table =  env["SQL_TABLE"]
    sql_table_prefix = env["SQL_TABLE_PREFIX"]
    sql_table_suffix = env["SQL_TABLE_SUFFIX"]

    sql_table_full = (
        (sql_table_prefix + "_" if sql_table_prefix else "") +
        (sql_table) +
        ("_" + sql_table_suffix if sql_table_suffix else "")
    )

    return sql_table_full

# Build the delete query to remove duplicate data
def get_delete_query(date_start, date_end, sites, env):

    sql_database =  env["SQL_DATABASE"]
    sql_schema =  env["SQL_SCHEMA"]
    sql_table = get_full_table_name(env)

    if sql_table == "uec_daily_smart":
        query = f"""
                DELETE FROM [{sql_database}].[{sql_schema}].[{sql_table}] 
                WHERE reportDate >= '{date_start}' 
                AND reportDate <= '{date_end}'
                """
    else:
        query = f"""
                DELETE FROM [{sql_database}].[{sql_schema}].[{sql_table}] 
                WHERE date_data >= '{date_start}' 
                AND date_data <= '{date_end}'
                """
    
    if sites:
        sites_string = ""
        for site in sites:
            sites_string += f"'{site}', "
        
        query += f"AND provider_code IN  ({sites_string[:-2]})"
    
    return query

'''
This needs unesting
'''

#Upload the request data
def upload_request_data(data, query_del, env, chunks=100):

    sql_table = get_full_table_name(env)
    

    #Upload the data
    try:
        #Connect to the database
        engine = snips.connect(env["SQL_ADDRESS"], env["SQL_DATABASE"])
        if (snips.table_exists(engine, sql_table, env["SQL_SCHEMA"])):
            #Delete the existing data
            snips.execute_query(engine, query_del)
        #Upload the new data
        snips.upload_to_sql(data, engine, sql_table, env["SQL_SCHEMA"], replace=False, chunks=chunks)
    except pyodbc.OperationalError:
        print("Disconnected from the sandpit. Waiting before trying again...")
        #If the connection drops, wait and try again
        add_delay(env["WAIT_COOLOFF"])

        try:
            #Connect to the database
            engine = snips.connect(env["SQL_ADDRESS"], env["SQL_DATABASE"])
            if (snips.table_exists(engine, sql_table, env["SQL_SCHEMA"])):
                #Delete the existing data
                snips.execute_query(engine, query_del)
            #Upload the new data
            snips.upload_to_sql(data, engine, sql_table, env["SQL_SCHEMA"], replace=False, chunks=chunks)
        except pyodbc.OperationalError as e:
            raise Exception("Connectioned dropped again so cancelling execution")
        except pyodbc.ProgrammingError as e:
            raise Exception (e)
    except pyodbc.ProgrammingError as e:
            raise Exception (e)