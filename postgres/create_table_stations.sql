CREATE TABLE climate_viewer.lkp.stations (
	stationid varchar(20) PRIMARY KEY not null,
	latitude decimal(8, 4) not null,
	longitude decimal(8, 4) not null,
	elevation decimal(6, 1),
	state_code char(2),
	station_name varchar(30) not null,
	gsn_flag char(3),
	hcn_crn_flag char(3),
	wmo_id integer
)


/*
IV. FORMAT OF "ghcnd-stations.txt"

------------------------------
Variable   Columns   Type
------------------------------
ID            1-11   Character - varchar(20) to be safe
LATITUDE     13-20   Real - decimal(8, 4)
LONGITUDE    22-30   Real - decimal(8, 4)
ELEVATION    32-37   Real - decimal(6, 1)
STATE        39-40   Character - char(2)
NAME         42-71   Character - varchar(30)
GSN FLAG     73-75   Character - char(3)
HCN/CRN FLAG 77-79   Character - char(3)
WMO ID       81-85   Character - really seems to be an integer
------------------------------
*/