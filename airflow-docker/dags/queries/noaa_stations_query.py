
noaa_stations_query = """
  DELETE FROM climate_viewer.lkp.stations;
  
  INSERT INTO climate_viewer.lkp.stations (
    {station_columns}
  )
  SELECT
    {station_columns}
  FROM
    df
"""
