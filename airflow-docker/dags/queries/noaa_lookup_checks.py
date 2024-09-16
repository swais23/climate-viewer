stations_freshness = """
    SELECT stationid
    FROM climate_viewer.raw.noaa_daily
    WHERE noaa_date = cast(strptime('{execution_date}', '%Y-%m-%d') AS DATE)
    EXCEPT
    SELECT stationid
    FROM climate_viewer.lookup.stations
"""

countries_freshness = """
    SELECT left(stationid, 2) AS country_code
    FROM climate_viewer.raw.noaa_daily
    WHERE noaa_date = cast(strptime('{execution_date}', '%Y-%m-%d') AS DATE)
    EXCEPT
    SELECT country_code
    FROM climate_viewer.lookup.countries
"""

states_freshness = """
    SELECT state_code
    FROM climate_viewer.lookup.stations
    WHERE state_code IS NOT NULL
    EXCEPT
    SELECT state_code
    FROM climate_viewer.lookup.states
"""
