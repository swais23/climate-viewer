CREATE TABLE climate_viewer.raw.noaa_daily_raw (
	stationid varchar(20) not null,
	noaa_date date not null,
	"element" char(4) not null,
	data_value integer not null,
	m_flag char(1),
	q_flag char(1),
	s_flag char(1),
	obs_time integer,
	PRIMARY KEY (stationid, noaa_date, element)
)