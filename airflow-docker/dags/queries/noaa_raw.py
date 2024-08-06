noaa_raw_query = """
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
      DATE BETWEEN '{start_date}' AND '{end_date}'
"""
