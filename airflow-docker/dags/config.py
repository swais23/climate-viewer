from collections import namedtuple
from dataclasses import dataclass
from typing import List, NamedTuple


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

_STATIONS_FILE_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"
_COUNTRIES_FILE_URL = "https://noaa-ghcn-pds.s3.amazonaws.com/ghcnd-countries.txt"
_STATES_FILE_URL = "https://noaa-ghcn-pds.s3.amazonaws.com/ghcnd-states.txt"

# Store columns from fixed-width files as defined in https://www.ncei.noaa.gov/pub/data/ghcn/daily/readme.txt
_Column = namedtuple('Column', ['column_name', 'start_position', 'end_position'])

# File format as defined under 'IV. FORMAT OF "ghcnd-stations.txt"'
_STATIONS_FILE_DEFINITION = [
  _Column('stationid', 0, 11),
  _Column('latitude', 12, 20),
  _Column('longitude', 21, 30),
  _Column('elevation', 31, 37),
  _Column('state_code', 38, 40),
  _Column('station_name', 41, 71),
  _Column('gsn_flag', 72, 75),
  _Column('hcn_crn_flag', 76, 79),
  _Column('wmo_id', 80, 85)
]

# File format as defined under 'V. FORMAT OF "ghcnd-countries.txt"'
_COUNTRIES_FILE_DEFINITION = [
  _Column('country_code', 0, 2),
  _Column('country_name', 3, 64)
]

# File format as defined under 'VI. FORMAT OF "ghcnd-states.txt"'
_STATES_FILE_DEFINITION = [
  _Column('state_code', 0, 2),
  _Column('state_name', 3, 50)
]

@dataclass
class NoaaLookup:
  file_url: str
  column_definition: List[NamedTuple]
  table_name: str

NOAA_LOOKUP_CONFIG = [
  NoaaLookup(_STATIONS_FILE_URL, _STATIONS_FILE_DEFINITION, 'stations'),
  NoaaLookup(_COUNTRIES_FILE_URL, _COUNTRIES_FILE_DEFINITION, 'countries'),
  NoaaLookup(_STATES_FILE_URL, _STATES_FILE_DEFINITION, 'states')
]