#%%
import configparser
import cx_Oracle
import io
import json
import numpy as np
import os
import pandas as pd
import requests

from bs4 import BeautifulSoup
from datetime import datetime
from retrying import retry
from zipfile import ZipFile


start_time = datetime.now()

# Connect to Oracle Autonomous Data Warehouse using Wallet and local config file for user/pw storage
# If using a different Oracle Client version, update the below path as needed
cx_Oracle.init_oracle_client(lib_dir=".venv/instantclient_19_10")
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
        if log_filename == 'new bikes':
            sql_insert_bikes(data, log_filename, current_file, False)  # Rerun the SQL statement
            connection.commit()  # Commit the batch insert to the DB
            print(f'{data_count - len(batcherror)} bike usages have been inserted with errors removed.')
        elif log_filename == 'new ridership':
            sql_insert_ridership(data, log_filename, current_file, False)  # Rerun the SQL statement
            connection.commit()  # Commit the batch insert to the DB
            print(f'{data_count - len(batcherror)} riderships have been inserted with errors removed.')
        elif log_filename == 'historical stations':
            sql_insert_station(data, log_filename, current_file, False)  # Rerun the SQL statement
            connection.commit()  # Commit the batch insert to the DB
            print(f'{data_count - len(batcherror)} stations have been inserted with errors removed.')
        elif log_filename == 'dates':
            sql_insert_date(data, log_filename, current_file, False)  # Rerun the SQL statement
            connection.commit()  # Commit the batch insert to the DB
            print(f'{data_count - len(batcherror)} dates have been inserted with errors removed.')
        elif log_filename == 'users':
            sql_insert_user(data, log_filename, current_file, False)  # Rerun the SQL statement
            connection.commit()  # Commit the batch insert to the DB
            print(f'{data_count - len(batcherror)} users have been inserted with errors removed.')
        elif log_filename == 'new routes':
            sql_insert_route(data, log_filename, current_file, False)  # Rerun the SQL statement
            connection.commit()  # Commit the batch insert to the DB
            print(f'{data_count - len(batcherror)} users have been inserted with errors removed.')
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
def sql_insert_bikes(data, log_filename, current_file, first_round = True):
    cur.executemany("""
        INSERT INTO admin.bikeusage_fact (bike_id, routepath_id, station_id_s, station_id_e, date_id, duration)
        VALUES(:1, :2, :3, :4, :5, :6) """, data, batcherrors=True)
    bad_data = len(cur.getbatcherrors())
    if bad_data == 0 and first_round:
        connection.commit()
        print(f'{len(data)} bike usage have been inserted without error.')
    elif bad_data > 0:
        remove_bad_obj(data, cur.getbatcherrors(), log_filename, current_file)
    else:
        pass
    return bad_data

@retry(wait_fixed=30000, stop_max_attempt_number=3)
def sql_insert_ridership(data, log_filename, current_file, first_round = True):
    cur.executemany("""
        INSERT INTO admin.ridership_fact (user_id, station_id_s, station_id_e, date_id, duration)
        VALUES(:1, :2, :3, :4, :5) """, data, batcherrors=True)
    bad_data = len(cur.getbatcherrors())
    if bad_data == 0 and first_round:
        connection.commit()
        print(f'{len(data)} ridership have been inserted without error.')
    elif bad_data > 0:
        remove_bad_obj(data, cur.getbatcherrors(), log_filename, current_file)
    else:
        pass
    return bad_data


@retry(wait_fixed=30000, stop_max_attempt_number=3)
def sql_insert_station(data, log_filename, current_file):
    cur.executemany("""
        INSERT into admin.station_dimension (station_id, station_name, station_latitude, station_longitude, zipcode, neighborhood, borough)
        VALUES(:1, :2, :3, :4, :5, :6, :7) """, data, batcherrors=True)
    if len(cur.getbatcherrors()) == 0:
        connection.commit()
        if len(data) > 0:
            print(f'{len(data)} stations have been inserted.')
    else:
        remove_bad_obj(data, cur.getbatcherrors(), log_filename, current_file)

@retry(wait_fixed=30000, stop_max_attempt_number=3)
def sql_insert_date(data, log_filename, current_file):
    cur.executemany("""
        INSERT into admin.date_dimension (Date_ID, Ride_Day, Ride_Week, Ride_Month, Ride_Year, Weekdays)
        VALUES(:1, :2, :3, :4, :5, :6) """, data, batcherrors=True)
    if len(cur.getbatcherrors()) == 0:
        connection.commit()
        if len(data) > 0:
            print(f'{len(data)} dates have been inserted.')
    else:
        remove_bad_obj(data, cur.getbatcherrors(), log_filename, current_file)

@retry(wait_fixed=30000, stop_max_attempt_number=3)
def sql_insert_user(data, log_filename, current_file):
    cur.executemany("""
        INSERT into admin.user_dimension (usertype, birth_year, age, gender, gendername)
        VALUES(:1, :2, :3, :4, :5) """, data, batcherrors=True)
    if len(cur.getbatcherrors()) == 0:
        connection.commit()
        if len(data) > 0:
            print(f'{len(data)} users have been inserted.')
    else:
        remove_bad_obj(data, cur.getbatcherrors(), log_filename, current_file)

@retry(wait_fixed=30000, stop_max_attempt_number=3)
def sql_insert_route(data, log_filename, current_file):
    cur.executemany("""
        INSERT into admin.route_dimension (routepath_id, route_path_bor)
        VALUES(:1, :2) """, data, batcherrors=True)
    if len(cur.getbatcherrors()) == 0:
        connection.commit()
        if len(data) > 0:
            print(f'{len(data)} routes have been inserted.')
    else:
        remove_bad_obj(data, cur.getbatcherrors(), log_filename, current_file)

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
    # For this project, the data was limited to only NYC data and years starting from 2018
    for file in range(len(data_files) - 1):
        if 'JC' not in data_files[file].get_text():
            if '2021' in data_files[file].get_text() or '2020' in data_files[file].get_text() or '2019' in data_files[file].get_text() or '2018' in data_files[file].get_text():
                zip_files.append(data_files[file].get_text())
except Exception as e:
    print(f"Failed {e}")

# Check if files to be downloaded has already been processed
processed = [filename for filename in cur.execute("SELECT filename FROM admin.data_processed")]
processed = [filename for tup in processed for filename in tup] # Convert a list of tuples to list of string
new_zips = sorted(set(zip_files) - set(processed) - set(['201307-201402-citibike-tripdata.zip']), reverse = True) # 201307-201402-citibike-tripdata.zip has its contents extracted already, remove duplicate effort

print(f'Identified {len(new_zips)} new file(s).')

total_records = 0

# Loop to visit all new identified links on Citibike data website
for zip_filename in new_zips:
    
    # Downloads and extracts the zip files into memory
    extracted = download_extract_zip(url + zip_filename)

    
    # Loop that goes through all files in the zip extract
    for file in extracted:
        # This code will update the processed list to make sure duplicate files are not reprocessed
        processed = [filename for filename in cur.execute("SELECT filename FROM admin.data_processed")]
        processed = [filename for tup in processed for filename in tup]  # Convert a list of tuples to list of string

        filename = file[0]
        fileobj = file[1]
        
        if filename.endswith(".csv") and filename not in processed and 'MACOSX' not in filename:  # Only process valid csv files not found in DB TABLE data_processed
            print(f'\nProcessing {filename}')
            
            # Get list of available stations in DB TABLE station_dimension
            # This code is inside the loop after each zip file download to get an updated list of available stations in the DB
            avail_station_id = [id for id in cur.execute("SELECT station_id FROM admin.station_dimension")]
            avail_station_id = [id for tup in avail_station_id for id in tup]  # Convert a list of tuples to list of numbers
            bad_records = 0

            # Creates an empty dataframe to store station information
            station_schema = ['station id', 'station name', 'station longitude', 'station latitude']
            stations = pd.DataFrame(columns=station_schema)

            # Read the file object from memory and load into a Pandas df
            df = pd.read_csv(fileobj, encoding='cp1252')
            original_row_count = df.shape[0]
            total_records += original_row_count
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
            df['birth year'].fillna(1800, inplace=True) # Changes NaN to 1800
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
                df['starttime'] = pd.to_datetime(reform_date).dt.strftime('%Y-%m-%d') #+ ' ' + df['starttime'].str.split(' ').str[1] # the time is excluded, uncomment if needed
                reform_date = df['stoptime'].str.split(' ').str[0]
                df['stoptime'] = pd.to_datetime(reform_date).dt.strftime('%Y-%m-%d') #+ ' ' + df['stoptime'].str.split(' ').str[1] # the time is excluded, uncomment if needed


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
            stations = stations.dropna()
            stations = stations[~stations['station id'].isin(avail_station_id)]  # Filter out station id that already exists in the DB station
            
            # Use Google Reverse Geocode API and run sql_insert_station if there are historical stations to add
            if stations.shape[0] > 0:
                # Create a list of lat/long pair
                coord = (stations['station longitude'].astype(str) + ',' + stations['station latitude'].astype(str)).tolist()
                filter_results = 'postal_code|neighborhood|sublocality'
                zipcodes = []

                city_df = pd.DataFrame(columns=['zipcode', 'neighborhood', 'borough'])

                # Loop goes through each lat/long pair to send a get request to Google Map reverse geocode API
                # This will try to match a zip code to the lat/long pair            

                for row, pair in enumerate(coord):
                    # Google Map reverse geocode API URL
                    gg_r = requests.get(f'{gg_url}?latlng={pair}&key={gg_api}&results={filter_results}')
                    geocode = json.loads(gg_r.text)


                    # These are flag variables created to check if data is found in the Google API request
                    postalcode = False
                    neighborhood = False
                    borough = False
                    all_flag = False


                    # A temp dataframe is used to store the relevant information
                    # If the zip code is not in the city_df, then it will be appended with the temp_df
                    temp_df = pd.DataFrame(columns=['zipcode', 'neighborhood', 'borough'])
                    # The nested for loop is used to populate the zip code column in station_df, which will be used to populate the DB table station
                    # It is also used to create the city_df, which will be used to populate the DB table city
                    for data in geocode['results']:
                        for add_comp in data['address_components']:
                            if 'postal_code' in add_comp['types'] and not postalcode:
                                postalcode = True
                                zipcodes.append(add_comp['long_name'])
                                temp_df.at[0,'zipcode'] = add_comp['long_name']
                                if city_df['zipcode'].isin([add_comp['long_name']]).any():
                                    all_flag = True
                                    break
                            elif 'neighborhood' in add_comp['types'] and not neighborhood:
                                neighborhood = True
                                temp_df.at[0,'neighborhood'] = add_comp['long_name']
                            elif 'sublocality' in add_comp['types'] and not borough:
                                borough = True
                                temp_df.at[0, 'borough'] = add_comp['long_name']
                            if postalcode and neighborhood and borough:
                                all_flag = True
                                city_df = city_df.append(temp_df, ignore_index=True) 
                                break
                        if all_flag:
                            break

                city_df = city_df.fillna('')

                # Add a new zip code column populating with data from the prior for loop
                stations['zip code'] = zipcodes

                # Perform a left join to merge the stations and the city df
                stations = stations.merge(city_df, left_on='zip code', right_on='zipcode', how='left')

                new_stations = stations[['station id', 'station name', 'station latitude', 'station longitude', 'zipcode', 'neighborhood', 'borough']]
                new_stations = new_stations.dropna()
                if stations.shape[0] > 0:
                    # Batch insert new station info into TABLE station_dimension
                    new_stations = new_stations.to_records(index=False).tolist()  # Convert df to a list of tuples
                    sql_insert_station(new_stations, 'historical stations', filename)
                    # remove_bad_obj(new_stations, station_error, 'historical stations', file[0])
            
            # Create a table date_dim
            df['starttime'] = pd.to_datetime(df['starttime']) # Convert to a datetime object to extract date values
            df['day'] = df['starttime'].dt.day
            df['week'] = df['starttime'].dt.isocalendar().week
            df['month'] = df['starttime'].dt.month
            df['year'] = df['starttime'].dt.year
            df['weekday'] = df['starttime'].dt.day_name()
            df['starttime'] = pd.to_datetime(df['starttime']).dt.strftime('%Y%m%d') # Convert datetime back to string

            # Reorder the date dimension table to match the schema of the DB
            date_dim = df[['starttime', 'day', 'week', 'month', 'year', 'weekday']].drop_duplicates()
            date_db = [dates[0] for dates in cur.execute("SELECT date_id FROM admin.date_dimension")] # Convert a list of tuples to a list of ints

            # Use a set comparision to identify new dates to be added
            new_dates = list(set(date_dim['starttime'].astype(int).to_list()) - set(date_db))

            if len(new_dates) > 0:
                # Batch insert date_dim into the DB TABLE date_dimension
                date_dim = date_dim[date_dim['starttime'].astype('int').isin(new_dates)]
                date_dim_db = date_dim.to_records(index=False).tolist()  # Convert df to a list of tuples
                sql_insert_date(date_dim_db, 'dates', filename)

            # Concatenate the start and end station names to create the unique route
            df['route_path'] = df['start station name'] + ' to ' + df['end station name']

            # Run a SQL query to get the borough information from the DB
            # Since borough information was obtained from the reverse geocode API, running it for each records would be expensive
            # This information is stored in the DB, either from etl_station_city.py or the earlier code to get historical stations
            updated_start_station = [stations for stations in cur.execute("SELECT station_id, borough FROM admin.station_dimension")]
            updated_start_station = pd.DataFrame(updated_start_station, columns=['start station id', 'start_borough'])
            updated_end_station = [stations for stations in cur.execute("SELECT station_id, borough FROM admin.station_dimension")]
            updated_end_station = pd.DataFrame(updated_end_station, columns = ['end station id', 'end_borough'])
            df = df.merge(updated_start_station, on='start station id', how='left')
            df = df.merge(updated_end_station, on='end station id', how='left')
            df['bor2bor'] = df['start_borough'] + ' to ' + df['end_borough']

            # Missing data values can cause issues when running the batch upload into the DB
            # Rather than trying to identifying and fixing these records, their count is miniscule compared to the overall count
            # It was simplier to just drop these records
            df = df.dropna()


            # Get an updated list of route paths from the DB
            # Do a set commparison to only add new routes identified in the raw data
            route_dim = [routes for routes in cur.execute("SELECT routepath_id, route_path_bor FROM admin.route_dimension")]
            route_df = df[['route_path', 'bor2bor']].drop_duplicates()
            route_list = route_df.to_records(index=False).tolist()  # Convert df to a list of tuples
            new_routes = list(set(route_list) - set(route_dim))
            sql_insert_route(new_routes, 'new routes', filename)

            # Create a df user_dim to populate into the DB TABLE User_Dimension
            user_dim = df[['usertype', 'birth year', 'gender']].drop_duplicates()
            user_dim['age'] = datetime.now().year - user_dim['birth year']
            user_dim = user_dim[['usertype', 'birth year', 'age', 'gender']]
            user_dim_list = user_dim.to_records(index=False).tolist()  # Convert df to a list of tuples
            user_dim_db = [user for user in cur.execute("SELECT usertype, birth_year, age, gender FROM admin.user_dimension")]

            # Only add users that do not exist in the DB
            new_users = list(set(user_dim_list) - set(user_dim_db))

            # Convert the list of new users to a df
            # This step is needed to help convert gender id to gender name
            new_users = pd.DataFrame(new_users, columns = ['usertype', 'birth year', 'age', 'gender'])
            conditions = [
                (new_users['gender'] == 0),
                (new_users['gender'] == 1),
                (new_users['gender'] == 2)]
            gender_name = ['Unknown', 'Male', 'Female']
            new_users['gendername'] = np.select(conditions, gender_name)
            new_users = new_users.to_records(index=False).tolist()  # Convert df to a list of tuples
            sql_insert_user(new_users, 'users', filename)

            # Since the DB is using an auto-generated ID as the primary key, we will need to query back the DB to get this value
            updated_user_dim_db = cur.execute("SELECT user_id, usertype, birth_year, age, gender FROM admin.user_dimension")
            updated_user_dim_db = [user for user in updated_user_dim_db]
            updated_user_df = pd.DataFrame(updated_user_dim_db, columns = ['user_id', 'usertype', 'birthyear', 'age', 'gender'])
            updated_user_df['gender'] = updated_user_df['gender'].astype(str).astype(int) # Need to convert column dtype from object to int

            # Merge the updated user df with main df to insert the user_id 
            df = df.merge(updated_user_df, left_on=['usertype', 'birth year', 'gender'], right_on=['usertype', 'birthyear', 'gender'], how='left')



            # Batch insert the ride info into the TABLE BikeUsage_Fact
            bikes = df[['bikeid', 'route_path', 'start station id', 'end station id', 'starttime', 'tripduration']]
            bikes = bikes.dropna() # Drop records that do not have a route path
            bikes['starttime'] = bikes['starttime'].astype(str).astype(int)
            bikes = bikes.to_records(index=False).tolist()  # Convert df to a list of tuples
            n = int(5e5)
            if len(bikes) > n:
                split_bikes = [bikes[i * n:(i + 1) * n] for i in range((len(bikes) + n - 1) // n)]  # Breaks up batch insert to size of n = 5e5
                for bikes_chunk in split_bikes:
                    bad_rows = sql_insert_bikes(bikes_chunk, 'new bikes', filename)
                    bad_records += bad_rows
                    # remove_bad_obj(rides_chunk, rides_error, 'new rides', file[0])
            else:
                bad_rows = sql_insert_bikes(bikes, 'new bikes', filename)
                bad_records += bad_rows
                # remove_bad_obj(rides, rides_error, 'new rides', file[0])
            

            # Batch insert the ride info into the TABLE Ridership_Fact
            ridership = df[['user_id', 'start station id', 'end station id', 'starttime', 'tripduration']]
            ridership = ridership.dropna() # Drop records in case there is a NaN
            ridership['starttime'] = ridership['starttime'].astype(str).astype(int)
            ridership = ridership.to_records(index=False).tolist()  # Convert df to a list of tuples
            n = int(5e5)
            if len(ridership) > n:
                split_ridership = [ridership[i * n:(i + 1) * n] for i in range((len(ridership) + n - 1) // n)]  # Breaks up batch insert to size of n = 5e5
                for ridership_chunk in split_ridership:
                    bad_rows = sql_insert_ridership(ridership_chunk, 'new ridership', filename)
                    # bad_records += bad_rows
                    # remove_bad_obj(rides_chunk, rides_error, 'new rides', file[0])
            else:
                bad_rows = sql_insert_bikes(ridership, 'new ridership', filename)
                # bad_records += bad_rows
                # remove_bad_obj(rides, rides_error, 'new rides', file[0])

    # Updated the TABLE data_processed
    cur.execute("""INSERT INTO admin.data_processed VALUES(:filename, :count)""", filename = zip_filename, count = bad_records)
    connection.commit()


cur.close()
connection.close()

end_time = datetime.now()
print(end_time - start_time)

print(f'Total records: {total_records}')