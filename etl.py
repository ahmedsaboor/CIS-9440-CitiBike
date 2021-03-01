#%%
import requests
from bs4 import BeautifulSoup
import shutil
import os
from zipfile import ZipFile
import cx_Oracle
import configparser
import pandas as pd

cleanup = False # Variable to control file deletion after processing

# Connect to Oracle Autonomous Data Warehouse using Wallet and local config file for user/pw storage
config = configparser.ConfigParser()
config.read('./auth/config.ini')
username = config.get('oracle', 'username')
password = config.get('oracle', 'password')
connection = cx_Oracle.connect(username, password, 'dwaproject_high')
cur = connection.cursor()

# Gets list of zip file names from the Citibike website
url = "https://s3.amazonaws.com/tripdata/"
r = requests.get(url)
soup = BeautifulSoup(r.text)
data_files = soup.find_all('key')
zip_files = []
try:
    for file in range(len(data_files) - 1):
        zip_files.append(data_files[file].get_text())
        # if '2013' in data_files[file].get_text() or '2014' in data_files[file].get_text() or '2015' in data_files[file].get_text():
        #     pass
        # else:
        #     zip_files.append(data_files[file].get_text())
except Exception as e:
    print(f"Failed {e}")


# Check if files to be downloaded has already been processed
processed = [filename for filename in cur.execute("SELECT * FROM admin.data_processed")]
processed = [filename for tup in processed for filename in tup] # Convert list of tuples to list of string
new_zips = list(set(zip_files) - set(processed))


# Downloads zip files into a subfolder called data_zipped
zipped = './data_zipped/'
# if not os.path.exists(zipped):
#     os.makedirs(zipped)
# print(f'Identified {len(new_zips)} new file(s).')
# for file in new_zips:
#     with open(zipped + file, 'wb') as f:
#         f.write(requests.get(url + file).content)
#         print(f'...downloading {file}')


# # Extracts all CSV files into a subfolder called data_unzipped
unzipped = './data_unzipped/'
# if not os.path.exists(unzipped):
#     os.makedirs(unzipped)
# for file in os.listdir(zipped):
#     print(f'...extracting {file}')
#     zip_ref = ZipFile(zipped + file)
#     zip_ref.extractall(unzipped)
#     zip_ref.close()
#     if cleanup:
#         os.remove(zipped + file) # Deletes zip after extraction
# shutil.rmtree('./data_unzipped/__MACOSX')  # Deletes an extraneous folder
# print(f'\nMACOSX folder deleted')

# Transforms and cleans the CSV files
station_schema = ['station id', 'station name', 'station longitude', 'station latitude']
stations = pd.DataFrame(columns = station_schema)

for file in os.listdir(unzipped):
    if file.endswith(".csv") and file not in processed:
        df = pd.read_csv(unzipped + file)
        print(f'\nProcessing {file}')

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
        df['birth year'] = pd.to_numeric(df['birth year'], errors='coerce') # Forces string values to NaN
        df['birth year'].fillna(0, inplace=True) # Changes NaN to 0
        miss_start = sum(df['start station id'].isna())
        if miss_start > 0:
            df.dropna(subset=['start station id'], inplace=True)
            print(f'{miss_start} rides were dropped.')
        miss_end = sum(df['end station id'].isna())
        if miss_end > 0:
            df.dropna(subset=['end station id'], inplace=True)
            print(f'{miss_end} rides were dropped.')


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


        # Builds a df of unique station info
        start_station = df.iloc[:, 3:7].drop_duplicates()
        start_station.columns = station_schema
        end_station = df.iloc[:, 7:11].drop_duplicates()
        end_station.columns = station_schema
        frames = [stations, start_station, end_station]
        stations = pd.concat(frames)
        stations.drop_duplicates(subset = 'station id', inplace = True, keep = 'last', ignore_index = True)
        stations.dropna(inplace = True)


        # Check for new station id not in table station
        new_station_id = stations['station id'].tolist()
        exist_stations = [station for station in cur.execute("select * from admin.station")]  # Creates a list of tuples from SQL query
        if len(exist_stations) == 0:
            exist_station_id = []
        else:
            exist_station_id = [station[0] for station in exist_stations]
        ids_to_add = list(set(new_station_id) - set(exist_station_id))
        new_stations = stations[stations['station id'].isin(ids_to_add)]
        

        # Batch insert new station info into table station
        new_stations = new_stations.to_records(index=False).tolist()  # Convert df to a list of tuples
        cur.executemany("""
            INSERT into admin.station 
            VALUES(:1, :2, :3, :4) """, new_stations, batcherrors=True)
        for error in cur.getbatcherrors():
            print("Error", error.message, "at row offset", error.offset)
        connection.commit()
        print(f'{len(new_stations) - len(cur.getbatcherrors())} stations has been inserted.')

        # Find existing stations that require update
        check_id = list(set(new_station_id) & set(exist_station_id))
        check_stations = stations[stations['station id'].isin(check_id)]
        check_stations = check_stations.to_records(index=False).tolist()  # Convert df to a list of tuples
        stations_to_update = list(set(check_stations) - set(exist_stations))
        stations_to_update = [list(tup) for tup in stations_to_update]  # Need to convert to list of list to use .pop method

        # Move the station id to the end of the list
        # This is due to bind variable is positional relative to the parameter
        # Since the where clause is at the end, we need to shift station id to the end of the list
        [lst.append(lst.pop(0)) for lst in stations_to_update]
        
        # Batch update new station info into table station
        cur.executemany("""
            UPDATE admin.station 
            SET stationname = :1, stationlat = :2, stationlong = :3
            WHERE stationid = :4 """, stations_to_update, batcherrors=True)
        for error in cur.getbatcherrors():
            print("Error", error.message, "at row offset", error.offset)
        connection.commit()
        print(f'{len(stations_to_update) - len(cur.getbatcherrors())} stations has been updated.')

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
                for error in cur.getbatcherrors():
                    print("Error", error.message, "at row offset", error.offset)
                connection.commit()
                print(f'{len(rides_chunk) - len(cur.getbatcherrors())} rides has been inserted.')
        else:
            cur.executemany("""
                INSERT INTO admin.ride (duration, starttime, stoptime, startstation, endstation, bikeid, usertype, birthyear, gender)
                VALUES(:1, TO_DATE(:2, 'YYYY-MM-DD HH24:MI:SS'), TO_DATE(:3, 'YYYY-MM-DD HH24:MI:SS'), :4, :5, :6, :7, :8, :9) """, rides, batcherrors=True)
            for error in cur.getbatcherrors():
                print("Error", error.message, "at row offset", error.offset)
            connection.commit()
            print(f'{len(rides) - len(cur.getbatcherrors())} rides has been inserted.')

        # Updated the TABLE data_processed
        cur.execute("""INSERT INTO admin.data_processed VALUES(:filename)""", filename = file + ".zip")
        connection.commit()

        if cleanup:
            os.remove(unzipped + file) # Delete CSV after processing

        # Save the updated file
        # df.to_csv(unzipped + file, index = None)
cur.close()
connection.close()
