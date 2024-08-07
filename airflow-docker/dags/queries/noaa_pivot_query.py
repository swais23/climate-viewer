
noaa_pivot_query = """
INSERT INTO climate_viewer.reporting.noaa_daily (
	noaa_date
	,stationid
	,{column_list}
)
SELECT
	to_date(noaa_date, 'YYYYMMDD') "noaa_date"
	,stationid
	,{noaa_pivot_case_statements}
FROM public.noaa_daily_raw
WHERE
	to_date(noaa_date, 'YYYYMMDD') BETWEEN '{start_date}' AND '{end_date}'
GROUP BY
	to_date(noaa_date, 'YYYYMMDD')
	,stationid
"""