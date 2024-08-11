noaa_raw_query = """
  DELETE FROM postgres_db.noaa_daily_raw
  WHERE
    strptime(noaa_date, '%Y%m%d') BETWEEN
    strptime('{start_date}', '%Y%m%d') AND strptime('{end_date}', '%Y%m%d');

  INSERT INTO postgres_db.noaa_daily_raw (
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
    FROM '{noaa_url}'
    WHERE
      -- duckdb interprets DATE (format='%Y%m%d') as BIGINT
      DATE BETWEEN '{start_date}' AND '{end_date}'
"""
