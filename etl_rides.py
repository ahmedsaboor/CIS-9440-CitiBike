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
from zipfile import ZipFile


start_time = datetime.now()


# Function to write SQL errors to a log file
def log_error(batch, process):
    if not os.path.exists('./log'):
        os.makedirs('./log')
    if len(batch) > 0:
        f = open(f'./log/{process}.txt', 'a')
        for error in batch:
            f.write(f'{datetime.now()} error, {error.message}, "at row offset, {error.offset}\n')
        f.close()
        print(f'Log file written with {len(batch)} errors to ./log/{process}.txt')


# Function to download and extract zip files into memory
def download_extract_zip(url):
    response = requests.get(url)
    with ZipFile(io.BytesIO(response.content)) as thezip:
        for zipinfo in thezip.infolist():
            with thezip.open(zipinfo) as thefile:
                yield zipinfo.filename, thefile


# Connect to Oracle Autonomous Data Warehouse using Wallet and local config file for user/pw storage
config = configparser.ConfigParser()
config.read('./auth/config.ini')
username = config.get('oracle', 'username')
password = config.get('oracle', 'password')
connection = cx_Oracle.connect(username, password, 'dwaproject_high')
cur = connection.cursor()


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
processed = [filename for filename in cur.execute("SELECT * FROM admin.data_processed")]
processed = [filename for tup in processed for filename in tup] # Convert a list of tuples to list of string
new_zips = list(set(zip_files) - set(processed))


print(f'Identified {len(new_zips)} new file(s).')

# Loop to visit all new identified links on Citibike data website
for value in new_zips:
    # Get list of available stations in DB table station
    # This code is inside the loop after each zip file download to get an updated list of available stations in the DB
    avail_station_id = [id for id in cur.execute("SELECT stationid FROM admin.station")]
    avail_station_id = [id for tup in avail_station_id for id in tup] # Convert a list of tuples to list of numbers

    # Downloads and extracts the zip files into memory
    extracted = download_extract_zip(url + value)
    

    # Creates an empty stations dataframe
    station_schema = ['station id', 'station name', 'station longitude', 'station latitude']
    stations = pd.DataFrame(columns=station_schema)
    
    
    # Loop that goes through all files in the zip extract
    for file in extracted:
        if file[0].endswith(".csv") and file[0] not in processed: # Only process csv files not found in DB table data_processed
            df = pd.read_csv(file[1])
            print(f'\nProcessing {file[0]}')
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
                print(f'{miss_start} rides were dropped due to missing start station id.')
            miss_end = sum(df['end station id'].isna())
            if miss_end > 0:
                df.dropna(subset=['end station id'], inplace=True)
                print(f'{miss_end} rides were dropped due to only missing end station id.')

            # There are some dummy station information, where lat/long is 0
            # These records were dropped
            df = df[df['start station longitude'] != 0]
            df = df[df['start station latitude'] != 0]
            df = df[df['end station longitude'] != 0]
            df = df[df['end station latitude'] != 0]
            cleaned_row_count = df.shape[0]
            print(f'{original_row_count - cleaned_row_count} rows were dropped due to bad station data.')

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
            # This bit of code will look at stations not in the DB table station and insert the missing station information
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

            # Batch insert new station info into table station
            new_stations = stations.to_records(index=False).tolist()  # Convert df to a list of tuples
            cur.executemany("""
                INSERT into admin.station (stationid, stationname, stationlat, stationlong, zipcode)
                VALUES(:1, :2, :3, :4, :5) """, new_stations, batcherrors=True)
            log_error(cur.getbatcherrors(), 'historical stations')
            connection.commit()
            print(f'{len(new_stations) - len(cur.getbatcherrors())} stations have been inserted.')

            # # The stations found in the above code are only defunct stations
            # # Therefore, there is no need to update the data in the DB
            # # Find existing stations that require update
            # check_id = list(set(new_station_id) & set(exist_station_id))
            # check_stations = stations[stations['station id'].isin(check_id)]
            # check_stations = check_stations.to_records(index=False).tolist()  # Convert df to a list of tuples
            # stations_to_update = list(set(check_stations) - set(exist_stations))
            # stations_to_update = [list(tup) for tup in stations_to_update]  # Need to convert to list of list to use .pop method
            # Move the station id to the end of the list
            # This is due to bind variable is positional relative to the parameter
            # Since the where clause is at the end, we need to shift station id to the end of the list
            # [lst.append(lst.pop(0)) for lst in stations_to_update]
            # # Batch update new station info into table station
            # cur.executemany("""
            #     UPDATE admin.station 
            #     SET stationname = :1, stationlat = :2, stationlong = :3
            #     WHERE stationid = :4 """, stations_to_update, batcherrors=True)
            # for error in cur.getbatcherrors():
            #     print("Error", error.message, "at row offset", error.offset)
            # connection.commit()
            # print(f'{len(stations_to_update) - len(cur.getbatcherrors())} stations has been updated.')



            # Batch insert the ride info into the table ride
            rides = df[['tripduration', 'starttime', 'stoptime', 'start station id', 'end station id', 'bikeid', 'usertype', 'birth year', 'gender']]
            rides = rides.to_records(index=False).tolist()  # Convert df to a list of tuples
            n = int(5e5)
            if len(rides) > n:
                split_rides = [rides[i * n:(i + 1) * n] for i in range((len(rides) + n - 1) // n)]  # Breaks up batch insert to size of n = 5e5
                for rides_chunk in split_rides:
                    cur.executemany("""
                        INSERT INTO admin.ride (duration, starttime, stoptime, startstation, endstation, bikeid, usertype, birthyear, gender)
                        VALUES(:1, TO_DATE(:2, 'YYYY-MM-DD HH24:MI:SS'), TO_DATE(:3, 'YYYY-MM-DD HH24:MI:SS'), :4, :5, :6, :7, :8, :9) """, rides_chunk, batcherrors=True)
                    log_error(cur.getbatcherrors(), 'new rides')
                    connection.commit()
                    print(f'{len(rides_chunk) - len(cur.getbatcherrors())} rides have been inserted.')
            else:
                cur.executemany("""
                    INSERT INTO admin.ride (duration, starttime, stoptime, startstation, endstation, bikeid, usertype, birthyear, gender)
                    VALUES(:1, TO_DATE(:2, 'YYYY-MM-DD HH24:MI:SS'), TO_DATE(:3, 'YYYY-MM-DD HH24:MI:SS'), :4, :5, :6, :7, :8, :9) """, rides, batcherrors=True)
                log_error(cur.getbatcherrors(), 'new rides')
                connection.commit()
                print(f'{len(rides) - len(cur.getbatcherrors())} rides have been inserted.')

            # Updated the TABLE data_processed
            cur.execute("""INSERT INTO admin.data_processed VALUES(:filename)""", filename = file[0] + ".zip")
            connection.commit()

cur.close()
connection.close()

end_time = datetime.now()
print(end_time - start_time)