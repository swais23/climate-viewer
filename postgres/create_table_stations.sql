CREATE TABLE climate_viewer.lookup.stations (
	stationid varchar(20) PRIMARY KEY not null,
	latitude decimal(8, 4) not null,
	longitude decimal(8, 4) not null,
	elevation decimal(6, 1),
	state_code char(2),
	station_name varchar(30) not null,
	gsn_flag char(3),
	hcn_crn_flag char(3),
	wmo_id integer,
	geom geometry(Point, 4326) GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)) STORED
)