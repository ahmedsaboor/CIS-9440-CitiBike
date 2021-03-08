CREATE TABLE city (
	zipcode			varchar2(10),
	neighborhood		varchar2(50),
	borough			varchar2(20),
	city			varchar2(30),
	county			varchar2(30),
	usstate			varchar2(30),
	primary key(zipcode));

CREATE TABLE station (
	stationid		number NOT NULL,
	stationname		varchar2(60),
	stationlat 		number,
	stationlong		number, 
	stationtype		varchar2(20),
	shortname		varchar2(10),
	capacity		number,
	regionid		number,
	externalid		varchar2(36),
	legacyid		number,
	zipcode			varchar2(10),
	primary key (stationid));

CREATE TABLE gender (
	genderid		number NOT NULL,
	gender			varchar2(7),
	primary key (genderid));

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
