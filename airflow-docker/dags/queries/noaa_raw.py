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
      cast(strptime(cast(DATE AS STRING), '%Y%m%d') AS DATE),
      ELEMENT,
      DATA_VALUE,
      M_FLAG,
      Q_FLAG,
      S_FLAG,
      OBS_TIME
    FROM '{noaa_url}'
    WHERE
      -- duckdb interprets DATE (format='%Y%m%d') as BIGINT
      DATE BETWEEN '{start_date}' AND '{end_date}'
"""
