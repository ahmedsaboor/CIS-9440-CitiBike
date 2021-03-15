#%%
import configparser
import cx_Oracle
import io
import json
import os
import pandas as pd
import requests
import shutil

from bs4 import BeautifulSoup
from datetime import datetime
from retrying import retry
from zipfile import ZipFile


start_time = datetime.now()

# Connect to Oracle Autonomous Data Warehouse using Wallet and local config file for user/pw storage
config = configparser.ConfigParser()
config.read('./auth/config.ini')
username = config.get('oracle', 'username')
password = config.get('oracle', 'password')
connection = cx_Oracle.connect(username, password, 'dwaproject_high')
cur = connection.cursor()

# Function to download and extract zip files into memory
def download_extract_zip(url):
    response = requests.get(url)
    with ZipFile(io.BytesIO(response.content)) as thezip:
        for zipinfo in thezip.infolist():
            with thezip.open(zipinfo) as thefile:
                yield zipinfo.filename, thefile

# Function to remove bad records from the batch SQL statement
# Writes a log file to identify bad records for review
def remove_bad_obj(data, batcherror, log_filename, current_file):
    if len(batcherror) < 101:
        connection.rollback()  # Roll back any transactions made prior to error
        data_count = len(data)
        if not os.path.exists('./log'):
            os.makedirs('./log')
        
        # Write a log file with the SQL error and the offset positions
        f = open(f'./log/{log_filename}.txt', 'a')
        for error in batcherror:
            f.write(f'{datetime.now()}, {current_file}, {error.message}, "at row offset, {error.offset}\n')
        f.close()
        print(f'Log file written with {len(batcherror)} errors to ./log/{log_filename}.txt')
        
        bad_obj = [obj.offset for obj in batcherror] # Get a list of positional values where an error was encountered
        bad_obj.reverse() # Since offset is positional, the code is reversed to start from high to low
        for obj in bad_obj:
            data.pop(obj)  # Remove the records 
        if log_filename == 'new rides':
            sql_insert_rides(data, log_filename, current_file, False)  # Rerun the SQL statement
            connection.commit()  # Commit the batch insert to the DB
            print(f'{data_count - len(batcherror)} rides have been inserted with errors removed.')
        elif log_filename == 'historical stations':
            sql_insert_station(data, log_filename, current_file)  # Rerun the SQL statement
            connection.commit()  # Commit the batch insert to the DB
            print(f'{data_count - len(batcherror)} stations have been inserted with errors removed.')
    else:
        f = open(f'./log/{log_filename}.txt', 'a')
        f.write(f'{datetime.now()}, {current_file}, Over 100 errors, will skip this batch \n')
        f.close()
        print(f'Over 100 errors in {current_file} with {log_filename}, will skip this batch')

    
# The SQL insert statement is wrapped in a function to use the retry function
# The Oracle DB can return an error ORA-30036: unable to extend segment by 8 in undo
# This error is caused by the Oracle DB running out of tablespace in the undo table
# If an error is encountered, the code will wait 30 seconds before retrying and stop after 3 tries
# By allowing time to pass, it will allow the existing transaction to complete, so the entire batch is not lost
# A parameter called first_round is added to prevent some messages from being printed
# Since the sql_insert functions and remove_bad_obj are recursively nested, the flag is used to determine if it is at the top level
@retry(wait_fixed=30000, stop_max_attempt_number=3)
def sql_insert_rides(rides, log_filename, current_file, first_round = True):
    cur.executemany("""
        INSERT INTO admin.ride (duration, starttime, stoptime, startstation, endstation, bikeid, usertype, birthyear, gender)
        VALUES(:1, TO_DATE(:2, 'YYYY-MM-DD HH24:MI:SS'), TO_DATE(:3, 'YYYY-MM-DD HH24:MI:SS'), :4, :5, :6, :7, :8, :9) """, rides, batcherrors=True)
    bad_data = len(cur.getbatcherrors())
    if bad_data == 0 and first_round:
        connection.commit()
        print(f'{len(rides)} rides have been inserted without error.')
    elif bad_data > 0:
        remove_bad_obj(rides, cur.getbatcherrors(), log_filename, current_file)
    else:
        pass
    return bad_data

@retry(wait_fixed=30000, stop_max_attempt_number=3)
def sql_insert_station(stations, log_filename, current_file):
    cur.executemany("""
        INSERT into admin.station (stationid, stationname, stationlat, stationlong, zipcode)
        VALUES(:1, :2, :3, :4, :5) """, stations, batcherrors=True)
    if len(cur.getbatcherrors()) == 0:
        connection.commit()
        if len(stations) > 0:
            print(f'{len(stations)} stations have been inserted.')
    else:
        remove_bad_obj(stations, cur.getbatcherrors(), log_filename, current_file)



# Load Google Map API key
gg_api = config.get('google', 'api')
gg_url = 'https://maps.googleapis.com/maps/api/geocode/json'


# Gets list of zip file names from the Citibike website
url = "https://s3.amazonaws.com/tripdata/"
r = requests.get(url)
soup = BeautifulSoup(r.text, features='html.parser')
data_files = soup.find_all('key')
zip_files = []
try:
    for file in range(len(data_files) - 1):
        zip_files.append(data_files[file].get_text())
except Exception as e:
    print(f"Failed {e}")


# Check if files to be downloaded has already been processed
processed = [filename for filename in cur.execute("SELECT filename FROM admin.data_processed")]
processed = [filename for tup in processed for filename in tup] # Convert a list of tuples to list of string
new_zips = sorted(set(zip_files) - set(processed) - set(['201307-201402-citibike-tripdata.zip'])) # 201307-201402-citibike-tripdata.zip has its contents extracted already, remove duplicate effort

print(f'Identified {len(new_zips)} new file(s).')

# Loop to visit all new identified links on Citibike data website
for zip_filename in new_zips:
    # Get list of available stations in DB TABLE station
    # This code is inside the loop after each zip file download to get an updated list of available stations in the DB
    avail_station_id = [id for id in cur.execute("SELECT stationid FROM admin.station")]
    avail_station_id = [id for tup in avail_station_id for id in tup]  # Convert a list of tuples to list of numbers
    bad_records = 0

    # Downloads and extracts the zip files into memory
    extracted = download_extract_zip(url + zip_filename)
    

    # Creates an empty stations dataframe
    station_schema = ['station id', 'station name', 'station longitude', 'station latitude']
    stations = pd.DataFrame(columns=station_schema)
    
    
    # Loop that goes through all files in the zip extract
    for file in extracted:
        # This code will update the processed list to make sure duplicate files are not reprocessed
        processed = [filename for filename in cur.execute("SELECT filename FROM admin.data_processed")]
        processed = [filename for tup in processed for filename in tup]  # Convert a list of tuples to list of string

        filename = file[0]
        fileobj = file[1]
        
        if filename.endswith(".csv") and filename not in processed and 'MACOSX' not in filename:  # Only process valid csv files not found in DB TABLE data_processed
            print(f'\nProcessing {filename}')
            df = pd.read_csv(fileobj, encoding='cp1252')
            original_row_count = df.shape[0]
            # Standardizes the column names for all CSV files
            df = df.rename(columns=({'Trip Duration':'tripduration',
                                    'Start Time':'starttime',
                                    'Stop Time':'stoptime',
                                    'Start Station ID':'start station id',
                                    'Start Station Name':'start station name',
                                    'Start Station Latitude':'start station latitude',
                                    'Start Station Longitude':'start station longitude',
                                    'End Station ID':'end station id',
                                    'End Station Name':'end station name',
                                    'End Station Latitude':'end station latitude',
                                    'End Station Longitude':'end station longitude',
                                    'Bike ID':'bikeid',
                                    'User Type':'usertype',
                                    'Birth Year':'birth year',
                                    'Gender': 'gender'}))
            
            # Cleans NaN data
            df['usertype'].fillna('', inplace=True)
            df['birth year'] = pd.to_numeric(df['birth year'], errors='coerce') # Forces \N values to NaN
            df['birth year'].fillna(0, inplace=True) # Changes NaN to 0
            miss_start = sum(df['start station id'].isna())
            if miss_start > 0:
                df.dropna(subset=['start station id'], inplace=True)
            miss_end = sum(df['end station id'].isna())
            if miss_end > 0:
                df.dropna(subset=['end station id'], inplace=True)

            # There are some dummy station information, where lat/long is 0
            # These records were dropped
            df = df[df['start station longitude'] != 0]
            df = df[df['start station latitude'] != 0]
            df = df[df['end station longitude'] != 0]
            df = df[df['end station latitude'] != 0]
            cleaned_row_count = df.shape[0]
            bad_stations = original_row_count - cleaned_row_count
            bad_records += bad_stations
            if bad_stations > 0:
                print(f'{bad_stations} rides were dropped due to bad station data.')

            # Strips milliseconds from timestamp
            if (df["starttime"].str.len() > 19).any():
                df["starttime"] = df["starttime"].str.slice(stop=-5)
            if (df["stoptime"].str.len() > 19).any():
                df["stoptime"] = df["stoptime"].str.slice(stop=-5)
            

            # Standardizes all date format to YYYY-MM-DD
            if (df['starttime'].str.contains('/')).any():
                reform_date = df['starttime'].str.split(' ').str[0]
                df['starttime'] = pd.to_datetime(reform_date).dt.strftime('%Y-%m-%d') + ' ' + df['starttime'].str.split(' ').str[1]
                reform_date = df['stoptime'].str.split(' ').str[0]
                df['stoptime'] = pd.to_datetime(reform_date).dt.strftime('%Y-%m-%d') + ' ' + df['stoptime'].str.split(' ').str[1]


            # Historical/defunct stations are not available in the Citibike station JSON feed
            # This bit of code will look at stations not in the DB TABLE station and insert the missing station information
            # Current station information will be processed by etl_station_city.py
            
            # Build a stations df with unique, but defunct records
            start_station = df.iloc[:, 3:7].drop_duplicates()
            start_station.columns = station_schema
            end_station = df.iloc[:, 7:11].drop_duplicates()
            end_station.columns = station_schema
            frames = [stations, start_station, end_station]
            stations = pd.concat(frames)
            stations.drop_duplicates(subset = 'station id', inplace = True, keep = 'last', ignore_index = True)
            stations.dropna(inplace=True)
            stations = stations[~stations['station id'].isin(avail_station_id)]  # Filter out station id that already exists in the DB station
            

            # Create a list of lat/long pair
            coord = (stations['station longitude'].astype(str) + ',' + stations['station latitude'].astype(str)).tolist()
            filter_results = 'postal_code'
            zipcodes = []
            # Loop goes through each lat/long pair to send a get request to Google Map reverse geocode API
            # This will try to match a zip code to the lat/long pair
            for pair in coord:
                zip_flag = False # Flag to break early from continuing to loop through the API request
                gg_r = requests.get(f'{gg_url}?latlng={pair}&key={gg_api}&results={filter_results}')
                geocode = json.loads(gg_r.text)
                for data in geocode['results']:
                    for add_comp in data['address_components']:
                        if 'postal_code' in add_comp['types']:
                            zip_flag = True
                            zipcodes.append(add_comp['long_name'])
                            break
                    if zip_flag:
                        break
            
            # Add a new zip code column populating with data from the prior for loop
            stations['zip code'] = zipcodes

            # Batch insert new station info into TABLE station
            new_stations = stations.to_records(index=False).tolist()  # Convert df to a list of tuples
            sql_insert_station(new_stations, 'historical stations', filename)
            # remove_bad_obj(new_stations, station_error, 'historical stations', file[0])
            


            # Batch insert the ride info into the TABLE ride
            rides = df[['tripduration', 'starttime', 'stoptime', 'start station id', 'end station id', 'bikeid', 'usertype', 'birth year', 'gender']]
            rides = rides.to_records(index=False).tolist()  # Convert df to a list of tuples
            n = int(5e5)
            if len(rides) > n:
                split_rides = [rides[i * n:(i + 1) * n] for i in range((len(rides) + n - 1) // n)]  # Breaks up batch insert to size of n = 5e5
                for rides_chunk in split_rides:
                    bad_data = sql_insert_rides(rides_chunk, 'new rides', filename)
                    bad_records += bad_data
                    # remove_bad_obj(rides_chunk, rides_error, 'new rides', file[0])
            else:
                bad_data = sql_insert_rides(rides, 'new rides', filename)
                bad_records += bad_data
                # remove_bad_obj(rides, rides_error, 'new rides', file[0])

    # Updated the TABLE data_processed
    cur.execute("""INSERT INTO admin.data_processed VALUES(:filename, :count)""", filename = zip_filename, count = bad_records)
    connection.commit()

cur.close()
connection.close()

end_time = datetime.now()
print(end_time - start_time)