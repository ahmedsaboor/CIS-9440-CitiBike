# Software, Libraries, and API Used

- [Oracle Autonomous Data Warehouse](https://www.oracle.com/autonomous-database/autonomous-data-warehouse/)
- Python 3.8.5 
  - BeautifulSoup 
  - configparser 
  - cx_Oracle
  - datetime
  - io 
  - json 
  - numpy 
  - os 
  - pandas
  - requests
  - retrying
  - zipfile
- [Tableau Desktop](https://www.tableau.com/products/desktop)
- [Google Map Reverse Geocode API](https://developers.google.com/maps/documentation/geocoding/overview#ReverseGeocoding)

# Setup

Create an Oracle Data Warehouse using this [tutorial](http://holowczak.com/getting-started-with-oracle-autonomous-database-in-the-cloud/).

Once the database is running, run the create.sql statements to setup the tables and its relationships.

Create a folder called auth in the root directory.

Extract the contents of the database wallet file into this folder.

Create a config.ini file and populate it with the below lines.
```
[oracle]
username = username
password = password

[google]
api = api_key
```

The library cx_Oracle requires some .dll files. Download [Oracle Instant Client Basic Package](https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html) and extract the contents into your Python or virtual environment.

Update the Easy Connect String in etl*.py with the appropriate TNS name. 
