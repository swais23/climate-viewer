
NOAA_ELEMENTS = [
    ("ADPT", 'average_dew_point', 0.1),  # tenths of degrees C --> FLOAT
    ("AWDR", 'average_wind_direction', None),  # degrees --> integer
    ("AWND", 'average_wind_speed', 0.1),  # tenths of meters per second --> FLOAT
    ("PSUN", 'percent_possible_sunshine', 0.01),  # percent (divide by 100) --> DECIMAL(4, 3)
    ("PRCP", 'precipitation', 0.1),  # tenths of mm --> FLOAT
    ("SNOW", 'snowfall', None),  # mm --> integer
    ("SNWD", 'snow_depth', None),  # mm --> integer
    ("TMAX", 'max_temperature', 0.1),  # tenths of degrees C --> FLOAT
    ("TMIN", 'min_temperature', 0.1),  # tenths of degrees C --> FLOAT
    ("TAVG", 'avg_hourly_temperature', 0.1),  # tenths of degrees C --> FLOAT
    ("RHAV", 'avg_humidity', 0.01),  # percent (divide by 100) --> DECIMAL(4, 3)
    ("RHMN", 'min_humidity', 0.01),  # percent (divide by 100) --> DECIMAL(4, 3)
    ("RHMX", 'max_humidity', 0.01),  # percent (divide by 100) --> DECIMAL(4, 3)
    ("TSUN", 'sunshine_minutes', None),  # minutes --> integer
    ("WSFG", 'peak_gust_wind_speed', 0.1)  # tenths of meters per second --> FLOAT
]