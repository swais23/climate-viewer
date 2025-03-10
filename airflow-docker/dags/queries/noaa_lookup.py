noaa_lookup_query = """
    TRUNCATE TABLE climate_viewer.lookup.{table};

    INSERT INTO climate_viewer.lookup.{table} (
        {columns}
    )
    SELECT
        {columns}
    FROM
        df
"""
