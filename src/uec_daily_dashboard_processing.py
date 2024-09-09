import sys
import pathlib
import os 
from os import getenv
import ncl_sqlsnippets as snips
from datetime import datetime as dtt, timedelta
from dotenv import load_dotenv
import re
import pandas as pd
import numpy as np
from sqlalchemy import types

from utils.smart_api import *
from utils.live_tracker_extract import *
from utils.sandpit_management import *
from utils.global_params import *
from utils.visualisation_functions import *
from utils.network_management import *

### Load environment variables 
config = toml.load("./config.toml")
load_dotenv(override=True)
site_id_map = pd.read_csv("./lookups/org_lookup.csv")

### Load runtime settings
env_runtime = import_settings(config, "runtime")

### Generate file for intermediate wrangle:

pd.DataFrame([], 
             columns=['source', 'indicatorKeyName', 'site_code_ref', 
                      'ReportDate', 'metric_type', 'value']
             ).to_csv('inter.csv', mode='w', index=False, header=True)

#Line break in terminal
print()

'''
Pull from smart API
'''
if env_runtime["DATA_PROCESSING_SMARTAPI"]:
    print("#########   Processing SMART API Pipeline   #########")

    ### Import settings from the .env file
    env = import_settings(config, "smart_api")

    ### Process the settings to get the start and end dates & 
    ### determine how many runs are needed to get all the data
    date_end = process_date_end(env["DATE_END"])
    date_start = process_date_window(env["DATE_WINDOW"], date_end)
    runs = calculate_runs(date_start, date_end)

    ### Execute the runs on the API

    #Get request variables
    url = env["API_URL"]
    key = env["API_KEY"]
    hash_sites = env["SITES"]
    delay = env["WAIT_PERIOD"]
    cooloff = env["WAIT_COOLOFF"]

    #Set  True initially so no delay on first request
    init = True

    smart_id_map = site_id_map[site_id_map["dataset"] == "smart_api"]

    #Iterate through runs to get all of the data
    for run in runs:

        #Get dates for the run
        date_start = run[0]
        date_end = run[1]

        #Make a get request per site
        for site in hash_sites:

            #Delay after 1st run to prevent Too Many Requests Error
            if init:
                init = False
            else:
                add_delay(delay)

            try:
                res = smart_request(url, key, date_start, date_end, site)
            except:
                print("Overload so waiting...")
                add_delay(cooloff)
                try:
                    res = smart_request(url, key, date_start, date_end, site)
                except:
                    raise Exception("Failed twice so cancelling execution.")

            ##Convert API response data into universal format for future work
            inter = processing_data_for_storage(config, res, 
                                                date_start, date_end)

            inter.to_csv('inter.csv', mode='a', index=False, header=False)

            # this needs to be generic enough for all 4 pipelines
            res_for_upload = res.merge(smart_id_map, how="left", 
                                       left_on="siteId", 
                                       right_on="dataset_reference")
            res_for_upload.drop(["dataset", "dataset_reference"], axis=1)

            #Upload and manage datasets
            query_del = get_delete_query(date_start, date_end, [res_for_upload["provider_code"][0]], env)

            upload_request_data(res_for_upload, query_del, env)

            print(f"Upload successful for {site} for ", 
                  f"{date_start} to {date_end}")

    print("\n")  

'''
LAS import
'''
if env_runtime["DATA_PROCESSING_LASHANDOVER"]:

    print("#########   Processing LAS Handover Pipeline   #########")

    ### Import settings from the .env file
    env = import_settings(config, "las")
    las_data_dir = getenv("NETWORKED_DATA_PATH_LAS")

    # Load the sheet into a dataframe
    try:
        las_file_path = fetch_excel_file(las_data_dir, ext=".xlsb")
    except Exception as e:
        print(e)
    else:
        las_data = pd.read_excel(las_file_path, sheet_name= "Data_Ambulance_Handovers")

        las_data.rename(columns=clean_column_name, inplace=True)
        las_data.columns = map(str.lower, las_data.columns)

        # filter to keep only relevent data
        ## NCL only
        las_data = las_data.query('stp_code == "QMJ"').reset_index(drop = True) # NCL STP only
        las_data = las_data.drop(['stp_code', 'stp_short', 'weekday', 'id'], axis=1) # columns not needed 
        ## Metrics of interest only
        las_data = las_data.melt(id_vars = ['hospital_site', 'period'], var_name='indicatorKeyName', value_name='value') # lengthen data to allow filter
        IndicatorList = config["las"]["base"]['indicator_list'] # import metric list
        las_data = las_data.query('indicatorKeyName in @IndicatorList') # filter metrics list
        las_data = las_data[['period', 'hospital_site','indicatorKeyName','value']].reset_index(drop=True) 
        ## Period of interest only
        las_data['period'] = pd.to_datetime(las_data['period'], unit='D', origin='1899-12-30')#, errors='coerce')
        las_data['cutoff'] = pd.Timestamp(dtt.now()).date()-pd.to_timedelta(env["EXTRACT_NUMBER_OF_DAYS"], unit='d')
        las_data = las_data.query("period >= cutoff")
        ## Add site reference codes
        las_id_map = site_id_map[site_id_map["dataset"] == "las"]
        las_data = las_data.merge(las_id_map, how="left", left_on="hospital_site", right_on="dataset_reference")

        ## add to inter for graphing
        las_data['source'] = 'las'
        las_data['metric_type'] = 'actual'
        las_data.rename(columns={'period': 'date_data'}, inplace=True)
        las_data = las_data[['source', 'indicatorKeyName', 'provider_code', 'date_data', 'metric_type', 'value']]
        las_data.to_csv('inter.csv', mode='a', index=False, header=False)

        ## Sandpit upload
        # reshape for sandpit
        las_data = las_data.pivot(index=['date_data', 'provider_code'], columns='indicatorKeyName', values='value').reset_index()
        las_data = las_data.rename_axis(None, axis = 1)

        date_end = las_data.max().iloc[0].date()
        date_start = (date_end - timedelta(days=env["EXTRACT_NUMBER_OF_DAYS"]))

        # upload to sandpit - once suficiently generalised
        query_del = get_delete_query(date_start, date_end, ["RAL01", "RAL26", "RALC7", "RAP", "RKE", "RRV"], env)
        upload_request_data(las_data, query_del, env)
        print(f"Upload successful for las")

        if env["ARCHIVE_LAS"]:
            try:
                archive_data_file(las_file_path, las_data_dir, "las_handover", date_end.strftime("%Y-%m-%d"), ext=".xlsb")
            except FileExistsError:
                print(f"Unable to archive las file as there is already a file with {date_end.strftime("%Y-%m-%d")} data in the archive folder.")

    print("\n")

'''
Live Tracker
'''
if env_runtime["DATA_PROCESSING_LIVETRACKERS"]:

    print("#########   Processing Live Tracker Pipeline   #########")

    #Load base settings
    env = import_settings(config, "live_tracker")
    
    #Import the functions for processing the files

    datasets = ["mo", "p2", "vw"]
    new_data_files = scan_new_files(datasets, env)

    #Run the extract function for each dataset
    for ds in datasets:
        print(f"\nProcessing new {ds} data...")

        if new_data_files[ds] == []:
            print_status(404, None)
        else:
            ef_controller(ds, env, new_data_files[ds])

    print("\n")