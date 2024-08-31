
noaa_stations_query = """
  DELETE FROM climate_viewer.lookup.stations;
  
  INSERT INTO climate_viewer.lookup.stations (
    {station_columns}
  )
  SELECT
    {station_columns}
  FROM
    df
"""
