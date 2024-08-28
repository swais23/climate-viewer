noaa_raw_query = """
  DELETE FROM climate_viewer.raw.noaa_daily
  WHERE
    noaa_date BETWEEN cast(strptime('{start_date}', '%Y%m%d') AS DATE) 
    AND cast(strptime('{end_date}', '%Y%m%d') AS DATE);

  INSERT INTO climate_viewer.raw.noaa_daily (
    stationid, 
    noaa_date, 
    element, 
    data_value, 
    m_flag, 
    q_flag, 
    s_flag, 
    obs_time            
    )
    SELECT
      ID,
      DATE,
      ELEMENT,
      DATA_VALUE,
      M_FLAG,
      Q_FLAG,
      S_FLAG,
      OBS_TIME
    FROM read_csv(
      '{noaa_url}',
      header = true,
      columns = {raw_columns},
      dateformat = '%Y%m%d'
      )
    WHERE
      DATE BETWEEN cast(strptime('{start_date}', '%Y%m%d') AS DATE)
      AND cast(strptime('{end_date}', '%Y%m%d') AS DATE)
"""
