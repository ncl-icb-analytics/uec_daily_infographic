import datetime
from os import getenv
from dotenv import load_dotenv

#Function to load environmental settings
def import_settings(config, ds):

    load_dotenv(override=True)

    #Load runtime settings seperately
    if ds == "runtime":
        params_runtime ={
            "DATA_PROCESSING_SMARTAPI": (
                    {"True": True, "False": False}
                    [getenv("DATA_PROCESSING_SMARTAPI")]),
            "DATA_PROCESSING_LASHANDOVER": (
                    {"True": True, "False": False}
                    [getenv("DATA_PROCESSING_LASHANDOVER")]),
            "DATA_PROCESSING_LIVETRACKERS": (
                    {"True": True, "False": False}
                    [getenv("DATA_PROCESSING_LIVETRACKERS")]),
            "DATA_EXPORT": (
                    {"True": True, "False": False}
                    [getenv("DATA_EXPORT")])
        }
        return params_runtime
    
    else:
        #Load parameters common to all datasets
        params_ds_global ={
            "SQL_ADDRESS": getenv("SQL_ADDRESS"),
            "SQL_DATABASE": config["database"]["sql_database"],
            "SQL_SCHEMA": config["database"]["sql_schema"],
            "SQL_TABLE_PREFIX": config["database"]["sql_table_prefix"],
            "SQL_TABLE_SUFFIX": config["database"]["sql_table_suffix"],

            "WAIT_COOLOFF": config["database"]["sql_cooloff"]
        }

    #Load the dataset specific parameters
    if ds == "smart_api":
        params_ds_unique ={
                "API_URL": config["smart_api"]["base"]["api_url"],
                "API_KEY": getenv("SMART_API_KEY"),
                "SITES": config["smart_api"]["base"]["sites"],
                
                "DATE_END": config["smart_api"]["date"]["end"],
                "DATE_WINDOW": config["smart_api"]["date"]["window"],
                
                "WAIT_PERIOD": config["smart_api"]["wait"]["period"],

                "SQL_TABLE": config["smart_api"]["base"]["sql_table"]
            }
    
    elif ds == "las":
        params_ds_unique ={
                "ARCHIVE_LAS": config["las"]["base"]["archive_file"],
                "EXTRACT_NUMBER_OF_DAYS": config["las"]["base"]["extract_number_of_days"],

                "SQL_TABLE": config["las"]["base"]["sql_table"]
            }
    
    elif ds == "live_tracker":
        params_ds_unique ={
                "ARCHIVE_FILE": config["live_tracker"]["archive_file"],

                "mo" : config["live_tracker"]["mo"]["mo"],
                "p2" : config["live_tracker"]["p2"]["p2"],
                "vw" : config["live_tracker"]["vw"]["vw"],

                "MO_SHEET_NAME": config["live_tracker"]["mo"]["mo_sheet_name"],
                "MO_TABLE_HEADING": config["live_tracker"]["mo"]["mo_table_heading"],
                "MO_SQL_TABLE": config["live_tracker"]["mo"]["mo_sql_table"],
                "MO_DATE_OVERWRITE": config["live_tracker"]["mo"]["mo_date_overwrite"],

                "P2_SHEET_NAME": config["live_tracker"]["p2"]["p2_sheet_name"],
                "P2_SQL_TABLE": config["live_tracker"]["p2"]["p2_sql_table"],
                "P2_DATE_OVERWRITE": config["live_tracker"]["p2"]["p2_date_overwrite"],

                "VW_SHEET_NAME": config["live_tracker"]["vw"]["vw_sheet_name"],
                "VW_SQL_TABLE": config["live_tracker"]["vw"]["vw_sql_table"]
            }
        params_ds_unique["date_extract"] = datetime.date.today().strftime("%Y-%m-%d")

    #Raise error if dataset given is not listed above
    else:
        raise ValueError (f"{ds} is not supported.")
    
    #Return the combination of the global and dataset specific parameters
    return params_ds_global | params_ds_unique