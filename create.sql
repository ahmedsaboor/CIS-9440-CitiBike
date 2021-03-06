

CREATE TABLE Station_Dimension(
Station_ID			number,
Station_name		varchar2(70),
Station_latitude	number,
Station_longitude	number,
Zipcode				varchar2(12),
Neighborhood		varchar2(50),
Borough				varchar2(50),
primary key(Station_ID)
);


CREATE TABLE User_Dimension(
User_ID				number GENERATED BY DEFAULT ON NULL AS IDENTITY,
Usertype			varchar2(10),
Birth_Year			number,
Age					number,
Gender				number,
GenderName			varchar2(10),
primary key(User_ID)
);

CREATE TABLE Date_Dimension(
Date_ID				number,
Ride_Day			number,
Ride_Week			number,
Ride_Month			number,
Ride_Year			number,
Weekdays			varchar2(10),
primary key(Date_ID)
);

CREATE TABLE Route_Dimension(
RoutePath_ID		varchar2(120),
Route_Path_Bor		varchar2(50),
primary key (RoutePath_ID)
);

CREATE TABLE BikeUsage_Fact (
BikeUsage_ID		number GENERATED BY DEFAULT ON NULL AS IDENTITY,
Bike_ID				number,
RoutePath_ID		varchar2(120),
Station_ID_S		number,
Station_ID_E		number,
Date_ID				number,
Duration			number,
primary key (BikeUsage_ID),
foreign key(RoutePath_ID) references Route_Dimension(RoutePath_ID),
foreign key(Station_ID_S) references Station_Dimension(Station_ID),
foreign key(Station_ID_E) references Station_Dimension(Station_ID),
foreign key(Date_ID) references Date_Dimension(Date_ID)
);

CREATE TABLE Ridership_Fact(
Ridership_ID		number GENERATED BY DEFAULT ON NULL AS IDENTITY,
User_ID				number,
Station_ID_S		number,
Station_ID_E		number,
Date_ID				number,
Duration			number,
primary key(Ridership_ID),
foreign key(User_ID) references User_Dimension(User_ID),
foreign key(Station_ID_S) references Station_Dimension(Station_ID),
foreign key(Station_ID_E) references Station_Dimension(Station_ID),
foreign key(Date_ID) references Date_Dimension(Date_ID)
);

CREATE TABLE data_processed (
filename		varchar2(50),
bad_records		number,
primary key (filename));
