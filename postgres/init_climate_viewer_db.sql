-- Databases
/* 
Don't need to create climate_viewer database since already set as default database in Docker compose
file and gets created automatically.
*/

\connect climate_viewer

-- Schemas
CREATE SCHEMA reporting;

CREATE SCHEMA "raw";

CREATE SCHEMA lookup;

-- Tables
CREATE TABLE climate_viewer.raw.noaa_daily (
	stationid varchar(20) not null,
	noaa_date date not null,
	"element" char(4) not null,
	data_value integer not null,
	m_flag char(1),
	q_flag char(1),
	s_flag char(1),
	obs_time integer,
	PRIMARY KEY (stationid, noaa_date, element)
);

CREATE TABLE climate_viewer.reporting.noaa_daily (
	stationid varchar(20) not null,
	noaa_date date not null,
	average_dew_point float,
	average_wind_direction integer,
	average_wind_speed float,
	percent_possible_sunshine decimal(4, 3),
	precipitation float,
	snowfall integer,
	snow_depth integer,
	max_temperature float,
	min_temperature float,
	avg_hourly_temperature float,
	avg_humidity decimal(4, 3),
	min_humidity decimal(4, 3),
	max_humidity decimal(4, 3),
	sunshine_minutes integer,
	peak_gust_wind_speed float,
	PRIMARY KEY (stationid, noaa_date)
);

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
);

CREATE TABLE climate_viewer.lookup.countries (
    country_code char(2) PRIMARY KEY not null,
    country_name varchar(100) not null
);

CREATE TABLE climate_viewer.lookup.states (
    state_code char(2) PRIMARY KEY not null,
    state_name varchar(100) not null
);

-- Indexes
CREATE INDEX stationid_date_element_idx ON climate_viewer.raw.noaa_daily (stationid, noaa_date, "element");

CREATE INDEX stationid_date_idx ON climate_viewer.reporting.noaa_daily (stationid, noaa_date);

CREATE INDEX stationid_idx ON climate_viewer.lookup.stations (stationid);

-- Extensions
CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA reporting;

COMMENT ON EXTENSION postgis IS 'PostGIS geometry and geography spatial types and functions';

SET default_tablespace = '';

SET default_table_access_method = heap;