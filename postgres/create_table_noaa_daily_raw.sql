CREATE TABLE climate_viewer.public.noaa_daily_raw (
	rawid integer primary key generated always as identity,
	stationid varchar(100) not null,
	noaa_date varchar(8) not null,
	"element" varchar(50) not null,
	data_value integer,
	m_flag varchar(10),
	q_flag varchar(10),
	s_flag varchar(10),
	obs_time integer
)