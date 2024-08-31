from collections import namedtuple


NOAA_RAW_COLUMNS = {
    'ID': 'VARCHAR',
    'DATE': 'DATE',
    'ELEMENT': 'VARCHAR',
    'DATA_VALUE': 'BIGINT',
    'M_FLAG': 'VARCHAR',
    'Q_FLAG': 'VARCHAR',
    'S_FLAG': 'VARCHAR',
    'OBS_TIME': 'INTEGER'
}

NOAA_ELEMENTS = [
  ("ADPT", 'average_dew_point', 0.1),  # tenths of degrees C --> FLOAT
  ("AWDR", 'average_wind_direction', 1),  # degrees --> integer
  ("AWND", 'average_wind_speed', 0.1),  # tenths of meters per second --> FLOAT
  ("PSUN", 'percent_possible_sunshine', 0.01),  # percent (divide by 100) --> DECIMAL(4, 3)
  ("PRCP", 'precipitation', 0.1),  # tenths of mm --> FLOAT
  ("SNOW", 'snowfall', 1),  # mm --> integer
  ("SNWD", 'snow_depth', 1),  # mm --> integer
  ("TMAX", 'max_temperature', 0.1),  # tenths of degrees C --> FLOAT
  ("TMIN", 'min_temperature', 0.1),  # tenths of degrees C --> FLOAT
  ("TAVG", 'avg_hourly_temperature', 0.1),  # tenths of degrees C --> FLOAT
  ("RHAV", 'avg_humidity', 0.01),  # percent (divide by 100) --> DECIMAL(4, 3)
  ("RHMN", 'min_humidity', 0.01),  # percent (divide by 100) --> DECIMAL(4, 3)
  ("RHMX", 'max_humidity', 0.01),  # percent (divide by 100) --> DECIMAL(4, 3)
  ("TSUN", 'sunshine_minutes', 1),  # minutes --> integer
  ("WSFG", 'peak_gust_wind_speed', 0.1)  # tenths of meters per second --> FLOAT
]

NOAA_STATIONS_FILE_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"

_Station = namedtuple('Station', ['column_name', 'start_position', 'end_position'])
# File format as defined under 'IV. FORMAT OF "ghcnd-stations.txt"' in https://www.ncei.noaa.gov/pub/data/ghcn/daily/readme.txt
NOAA_STATIONS_FILE_DEFINITION = [
  _Station('stationid', 0, 11),
  _Station('latitude', 12, 20),
  _Station('longitude', 21, 30),
  _Station('elevation', 31, 37),
  _Station('state_code', 38, 40),
  _Station('station_name', 41, 71),
  _Station('gsn_flag', 72, 75),
  _Station('hcn_crn_flag', 76, 79),
  _Station('wmo_id', 80, 85)
]
