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
station_df = station_df[['station_id', 'name', 'lat', 'lon', 'station_type',
                        'short_name', 'capacity', 'region_id', 'external_id', 'legacy_id']]
station_df['station_id'] = pd.to_numeric(station_df['station_id'], errors='coerce')
station_df['region_id'] = pd.to_numeric(station_df['region_id'], errors='coerce')
station_df['legacy_id'] = pd.to_numeric(station_df['legacy_id'], errors='coerce')
station_df['region_id'].fillna(0, inplace=True)


# # Load MapQuest API key
# mq_api = config.get('mapquest', 'api')
# mq_url = f'http://www.mapquestapi.com/geocoding/v1/batch?key={mq_api}'



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
# Add the zip codes to the station_df
station_df['zipcode'] = zipcodes


# # Look up zip code based on geographic coordinates using MapQuest Reverse Geocode API
# zipcodes = []
# coord = (station_df['lat'].astype(str) + ',' + station_df['lon'].astype(str)).tolist()
# coord_string = ''
# # Loop that goes through lat/long pair and sends a get batch request of size 100 to MapQuest
# for count, pair in enumerate(coord):
#     if count % 100 != 99 and count != len(coord) - 1: # if batch size is not 100 and if location is not the last in the list, concat the URL parameter
#         coord_string += '&location=' + pair
#     elif count % 100 == 99 or count == len(coord) - 1: # if batch size is 100 or if location is last is the list, then send the get request
#         coord_string += '&location=' + pair
#         mq_r = requests.get(mq_url + coord_string)
#         mq_raw_data = json.loads(mq_r.text)['results']
#         #zips = [loc['locations'][0]['postalCode'][-5:] if len(loc['locations'][0]['postalCode']) < 10 else loc['locations'][0]['postalCode'][0:5] for loc in mq_raw_data]
#         zips = [loc['locations'][0]['postalCode'][0:5] for loc in mq_raw_data]
#         zipcodes.extend(zips)
#         zips.clear()
#         coord_string = ''
# # Add the zip codes to the df
# station_df['zipcode'] = zipcodes


# Check for new station id not in table station
new_station_id = station_df['station_id'].tolist()
station_db = [station for station in cur.execute("select * from admin.station")]  # Creates a list of tuples from SQL query
if len(station_db) == 0:
    exist_station_id = []
else:
    exist_station_id = [station[0] for station in station_db]
ids_to_add = list(set(new_station_id) - set(exist_station_id))
new_stations = station_df[station_df['station_id'].isin(ids_to_add)]

# Batch insert new station info into table station
if new_stations.shape[0] > 0: # only run insert SQL code if there are stations to insert
    stations = new_stations.to_records(index=False).tolist() # Convert df to a list of tuples
    cur.executemany("""
        INSERT INTO admin.station
        VALUES(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)""", stations, batcherrors=True)
    log_error(cur.getbatcherrors(), 'new station')
    connection.commit()
    print(f'{len(stations) - len(cur.getbatcherrors())} stations has been inserted.')

# Find existing stations that require update
check_id = list(set(new_station_id) & set(exist_station_id))
check_stations = station_df[station_df['station_id'].isin(check_id)]
check_stations = check_stations.to_records(index=False).tolist()  # Convert df to a list of tuples
stations_to_update = list(set(check_stations) - set(station_db))

if len(stations_to_update) > 0: # only run update SQL code if there are stations to update
    stations_to_update = [list(tup) for tup in stations_to_update]  # Need to convert to list of list to use .pop method

    # Move the station_id to the end of the list
    # This is due to bind variables are positional relative to the parameter
    # Since the SQL where clause is at the end, we need to shift station_id to the end of the list
    [lst.append(lst.pop(0)) for lst in stations_to_update]

    # Batch update new station info into table station
    cur.executemany("""
        UPDATE admin.station 
        SET stationname = :1, stationlat = :2, stationlong = :3, stationtype = :4, shortname = :5, 
        capacity = :6, regionid = :7, externalid = :8, legacyid = :9, zipcode = :10
        WHERE stationid = :11 """, stations_to_update, batcherrors=True)
    log_error(cur.getbatcherrors(), 'update station')
    connection.commit()
    print(f'{len(stations_to_update) - len(cur.getbatcherrors())} stations has been updated.')



# The below code is commented out as it is only used when using the MapQuest API and they do not provide all the information
# # Get zip codes and their respective neighborhood and boroughs
# r = requests.get('https://www.health.ny.gov/statistics/cancer/registry/appendix/neighborhoods.htm')
# soup = BeautifulSoup(r.text, features='html.parser')
# nyc_data = [i.get_text().strip() for i in soup.find_all('td')] # Use list comprehension to get zip code and neighborhood data
# city_df = pd.DataFrame(columns=['zipcode', 'neighborhood', 'borough', 'city', 'county'])
# # Loop to fill a DataFrame with NYC zip codes with its respective neighborhood and borough
# # Table from NY DOH is not in 1NF, so this cumbersome loop was used to fill in the df
# # Since the HTML text is ordered with borough, neighborhood, then zip codes, the list was reversed
# # and a counter variable is used to keep track if it is a neighborhood or borough
# counter = 0
# for value in reversed(nyc_data):
#     value = value.split(',')
#     if value[0].isnumeric(): # if the first element is numeric, then it is a zip code
#         counter = 0
#         city_df = city_df.append(pd.DataFrame(value, columns=['zipcode']), ignore_index=True)
#     elif not value[0].isnumeric() and counter == 0: # if value is text and counter is 0, then it is a neighborhood
#         counter += 1
#         city_df['neighborhood'].fillna(value[0], inplace=True)
#     elif not value[0].isnumeric() and counter == 1: # if value is text and counter is 1, then it is a borough
#         city_df['borough'].fillna(value[0], inplace=True) 
# city_df['zipcode'] = city_df['zipcode'].str.strip()
# # Looks for zip codes not already in city_df and adds it as a new row
# new_zc = list(set(zipcodes) - set(city_df['zipcode'].tolist()))
# city_df = city_df.append(pd.DataFrame(new_zc, columns=['zipcode']), ignore_index=True)
# # Finds the city and county names for each zip code
# zc_url = 'https://www.unitedstateszipcodes.org/'
# h = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0'}
# states = ['nj', 'ny']
# for s in states:
#     zc_r = requests.get(zc_url + s, headers=h)
#     soup = BeautifulSoup(zc_r.text, features='html.parser')
#     # Loops through each row of the city_df to look up each zip code in the html data
#     # The layout of the html has the city name 2 tags after the zip code and the county name 1 tag after the city name
#     for row, zc in enumerate(city_df['zipcode']):
#         try: # try/except is used because looking up zip code in the incorrect state will cause a hard error
#             zc_html = soup.find_all('a', string=zc)[0]
#             city_html = zc_html.find_next().find_next()
#             city = city_html.get_text(strip=True)
#             city = city.split(',') # Some cities have multiple names listed, will only take the 1st entry
#             city = city[0]
#             county_html = city_html.find_next()
#             county = county_html.get_text(strip=True)
#             city_df.at[row, 'city'] = city
#             city_df.at[row, 'county'] = county
#         except Exception as e:
#             pass
# # Since not all fields are relavent for each zip code or no updated source can be found, NaN are filled as ''
# city_df.fillna('', inplace=True)

city_df.fillna('', inplace=True)
# Check for new zip codes/cities not in table city
new_zipcode = city_df['zipcode'].tolist()
city_db = [record for record in cur.execute("select * from admin.city")]  # Creates a list of tuples from SQL query
if len(city_db) == 0:
    exist_zipcode = []
else:
    exist_zipcode = [row[0] for row in city_db] # Zip codes are stored in the 1st column of the DB table. This builds a list of zip codes
zip_to_add = list(set(new_zipcode) - set(exist_zipcode))
new_cities = city_df[city_df['zipcode'].isin(zip_to_add)]


# Batch insert new zip codes and cities into table city
if new_cities.shape[0] > 0: # only run insert SQL code if there are cities to insert
    cities = new_cities.to_records(index=False).tolist() # Convert df to a list of tuples
    cur.executemany("""
        INSERT INTO admin.city
        VALUES(:1, :2, :3, :4, :5, :6)""", cities, batcherrors=True)
    log_error(cur.getbatcherrors(), 'new city')
    connection.commit()
    print(f'{len(cities) - len(cur.getbatcherrors())} stations has been inserted.')


# Find existing stations that require update
check_zc = list(set(new_zipcode) & set(exist_zipcode))
check_cities = city_df[city_df['zipcode'].isin(check_zc)]
check_cities = check_cities.to_records(index=False).tolist()  # Convert df to a list of tuples
cities_to_update = list(set(check_cities) - set(city_db))
cities_to_update = [list(tup) for tup in cities_to_update]  # Need to convert to list of list to use .pop method

# Move the zipcode to the end of the list
# This is due to bind variables are positional relative to the parameter
# Since the SQL where clause is at the end, we need to shift zipcode to the end of the list
[lst.append(lst.pop(0)) for lst in cities_to_update]

# Batch update zip codes and cities into the table city
cur.executemany("""
    UPDATE admin.city 
    SET neighborhood = :1, borough = :2, city = :3, county = :4, usstate = :5
    WHERE zipcode = :6 """, cities_to_update, batcherrors=True)
log_error(cur.getbatcherrors(), 'update city')
connection.commit()
print(f'{len(cities_to_update) - len(cur.getbatcherrors())} stations has been updated.')



cur.close()
connection.close()
