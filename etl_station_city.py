#%%
import requests
import json
import pandas as pd
import cx_Oracle
import configparser
from datetime import datetime
from bs4 import BeautifulSoup

# Function to write SQL batch errors to a log file
def log_error(batch, process):
    if not os.path.exists('./log'):
        os.makedirs('./log')
    if len(batch) > 0:
        f = open(f'./log/{process}.txt', 'a')
        for error in batch:
            f.write(f'{datetime.now()} error, {error.message}, "at row offset, {error.offset}\n')
        f.close()
        print(f'Log file written with {len(batch)} errors to ./log/{process}.txt')


# Connect to Oracle Autonomous Data Warehouse using Wallet and local config file for user/pw storage
cx_Oracle.init_oracle_client(lib_dir=".venv/instantclient_19_10")
config = configparser.ConfigParser()
config.read('./auth/config.ini')
username = config.get('oracle', 'username')
password = config.get('oracle', 'password')
connection = cx_Oracle.connect(username, password, 'dwaproject_high')
cur = connection.cursor()


# Get Citibike station information
cb_url = 'https://gbfs.citibikenyc.com/gbfs/en/station_information.json'
cb_r = requests.get(cb_url)
cb_raw_data = json.loads(cb_r.text)['data']['stations']

# Convert Citibike JSON data into dataframe
station_dict = {index: station for index, station in enumerate(cb_raw_data)}
station_df = pd.DataFrame.from_dict(station_dict, orient='index')

# Clean and reorganize the station dataframe
station_df = station_df[['station_id', 'name', 'lat', 'lon']]
station_df['station_id'] = pd.to_numeric(station_df['station_id'], errors='coerce')


# Load Google Map API key
gg_api = config.get('google', 'api')
gg_url = 'https://maps.googleapis.com/maps/api/geocode/json'


# Create a list of lat/long pair
coord = (station_df['lat'].astype(str) + ',' + station_df['lon'].astype(str)).tolist()
zipcodes = []

# This variable is used to fitler the API result from Google for zip code, neighborhood, borough, city, county, and state, respectively
filter_results = 'postal_code|neighborhood|sublocality|locality|administrative_area_level_2, administrative_area_level_1'

city_df = pd.DataFrame(columns=['zipcode', 'neighborhood', 'borough', 'city', 'county', 'state'])

# Loop goes through each lat/long pair to find the zip code, and its respective location information
for row, pair in enumerate(coord):
    # Google Map reverse geocode API URL
    gg_r = requests.get(f'{gg_url}?latlng={pair}&key={gg_api}&results={filter_results}')
    geocode = json.loads(gg_r.text)


    # These are flag variables created to check if data is found in the Google API request
    postalcode = False
    neighborhood = False
    borough = False
    city = False
    county = False
    state = False
    all_flag = False


    # A temp dataframe is used to store the relevant information
    # If the zip code is not in the city_df, then it will be appended with the temp_df
    temp_df = pd.DataFrame(columns=['zipcode', 'neighborhood', 'borough', 'city', 'county', 'state'])
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
            elif 'locality' in add_comp['types'] and not city:
                city = True
                temp_df.at[0,'city'] = add_comp['long_name']
            elif 'administrative_area_level_2' in add_comp['types'] and not county:
                county = True
                temp_df.at[0,'county'] = add_comp['long_name']
            elif 'administrative_area_level_1' in add_comp['types'] and not state:
                state = True
                if add_comp['long_name'] == 'New Jersey': # Borough is only for NY as NJ doesn't have any boroughs, so the variable is set True if NJ
                    borough = True 
                temp_df.at[0,'state'] = add_comp['long_name']
            if postalcode and neighborhood and borough and city and county and state:
                all_flag = True
                city_df = city_df.append(temp_df, ignore_index=True) 
                break
        if all_flag:
            break
city_df.fillna('', inplace=True)
city_df = city_df[['zipcode', 'neighborhood', 'borough']]

# Add the zip codes to the station_df
station_df['zipcode'] = zipcodes

# Perform a left join to merge the station df and the city df
full_station_df = station_df.merge(city_df, on='zipcode', how='left')


# Check for new station id not in table station
new_station_id = full_station_df['station_id'].tolist()
station_db = [station for station in cur.execute("select * from admin.station_dimension")]  # Creates a list of tuples from SQL query
if len(station_db) == 0:
    exist_station_id = []
else:
    # Extract the station id from the list of tuples
    exist_station_id = [station[0] for station in station_db]
ids_to_add = list(set(new_station_id) - set(exist_station_id))
new_stations = full_station_df[full_station_df['station_id'].isin(ids_to_add)]



# Batch insert new station info into table station_dimension
if new_stations.shape[0] > 0: # only run insert SQL code if there are new stations to insert
    stations = new_stations.to_records(index=False).tolist() # Convert df to a list of tuples
    cur.executemany("""
        INSERT INTO admin.station_dimension
        VALUES(:1, :2, :3, :4, :5, :6, :7)""", stations, batcherrors=True)
    log_error(cur.getbatcherrors(), 'new station')
    connection.commit()
    print(f'{len(stations) - len(cur.getbatcherrors())} stations has been inserted.')

# Find existing stations that require update
check_id = list(set(new_station_id) & set(exist_station_id))
check_stations_df = full_station_df[full_station_df['station_id'].isin(check_id)]
check_stations_df = check_stations_df.to_records(index=False).tolist()  # Convert df to a list of tuples
stations_to_update = list(set(check_stations_df) - set(station_db))

if len(stations_to_update) > 0:  # only run update SQL code if there are stations to update
    # Need to convert to list of list to utilize the .pop method
    stations_to_update = [list(tup) for tup in stations_to_update]  

    # Move the station_id to the end of the list
    # This is due to bind variables being positional relative to the parameter
    # Since the SQL WHERE clause is at the end, we need to shift station_id to the end of the list
    [lst.append(lst.pop(0)) for lst in stations_to_update]

    # Batch update new station info into table station_dimension
    cur.executemany("""
        UPDATE admin.station_dimension 
        SET station_name = :1, station_latitude = :2, station_longitude = :3, zipcode = :4, neighborhood = :5, borough = :6
        WHERE station_id = :7 """, stations_to_update, batcherrors=True)
    log_error(cur.getbatcherrors(), 'update station')
    connection.commit()
    print(f'{len(stations_to_update) - len(cur.getbatcherrors())} stations has been updated.')

cur.close()
connection.close()
