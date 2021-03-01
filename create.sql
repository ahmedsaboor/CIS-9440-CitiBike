CREATE TABLE station (
	stationid		number NOT NULL,
	stationname		varchar2(60),
	stationlat 		number,
	stationlong		number, 
	primary key (stationID));

CREATE TABLE gender (
	genderid		number NOT NULL,
	gender			varchar2(7),
	primary key (genderid)
);

CREATE TABLE ride (
	duration 		number,
	starttime 		date,
	stoptime 		date, 
	startstation 		number,
	endstation 		number,
	bikeid 			number, 
	usertype 		varchar2(10), 
	birthyear 		number,
	gender 			number, 
	primary key (bikeid, starttime),
	foreign key (startstation)	references station(stationid),
	foreign key (endstation) 	references station(stationid),
	foreign key (gender)		references gender(genderid));

CREATE TABLE data_processed (
	filename		varchar2(50),
	primary key (filename));

INSERT INTO gender VALUES (0, 'Unknown');
INSERT INTO gender VALUES (1, 'Male');
INSERT INTO gender VALUES (2, 'Female');
