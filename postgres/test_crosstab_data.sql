SELECT 
	cast(noaa_date AS date) "noaa_date"
	,stationid
	,"element"
	,SUM(data_value)
FROM public.noaa_daily_raw
WHERE
	"element" IN (
		"ADPT"
		,"ADSLP"
		,"MNPN"
		,"MXPN"
	)
GROUP BY
	cast(noaa_date AS date)
	,stationid
	,"element"
LIMIT 100;


SELECT 
	DISTINCT "element" 
FROM public.noaa_daily_raw 
-- ORDER BY "element"
WHERE
	"element" IN (
		'ADPT'
		,'ASLP'
		,'MNPN'
		,'MXPN'
	);


SELECT
	*
FROM crosstab(
	'SELECT 
		cast(noaa_date AS date) "noaa_date"
		,stationid
		,"element"
		,SUM(data_value)
	FROM public.noaa_daily_raw
	WHERE
		"element" IN (
			''ADPT''
			,''ASLP''
			,''MNPN''
			,''MXPN''
		)
	GROUP BY
		cast(noaa_date AS date)
		,stationid
		,"element"',
	'SELECT 
		DISTINCT "element" 
	FROM public.noaa_daily_raw 
	WHERE
		"element" IN (
			''ADPT''
			,''ASLP''
			,''MNPN''
			,''MXPN''
		)'
) AS ct (
	noaa_date date
	,stationid varchar(100)
	,"ADPT" int
	,"ADSLP" int
	,"MNPN" int
	,"MXPN" int
)
LIMIT 1000;