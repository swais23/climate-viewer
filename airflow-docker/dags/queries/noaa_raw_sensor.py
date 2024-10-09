# Use with SqlSensor to check whether row count > 0 for execution date
sensor_query = """
    SELECT COUNT(*) AS cnt
    FROM climate_viewer.raw.noaa_daily
    WHERE noaa_date = '{execution_date}'
"""
