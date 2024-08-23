
noaa_pivot_query = """
WITH SourceQuery AS (
	SELECT
		noaa_date
		,stationid
		,{noaa_pivot_case_statements}
	FROM climate_viewer.raw.noaa_daily
	WHERE
		noaa_date BETWEEN '{start_date}' AND '{end_date}'
	GROUP BY
		noaa_date
		,stationid
)


MERGE INTO climate_viewer.reporting.noaa_daily tgt
USING SourceQuery src
ON 
	src.stationid = tgt.stationid
	AND src.noaa_date = tgt.noaa_date
WHEN MATCHED THEN
	UPDATE SET
	{merge_update_statements}
WHEN NOT MATCHED THEN
	INSERT (stationid, noaa_date, {column_list})
	VALUES (src.stationid, src.noaa_date, {merge_insert_list})
"""
