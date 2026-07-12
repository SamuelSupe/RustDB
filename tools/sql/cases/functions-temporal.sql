SELECT extract(year FROM DATE '2024-02-29') AS extract_year,
       date_part('month', TIMESTAMP '2024-02-29 12:34:56.123456') AS part_month,
       year(DATE '2024-02-29') AS year_value,
       month(DATE '2024-02-29') AS month_value,
       day(DATE '2024-02-29') AS day_value,
       date_trunc('year', DATE '2024-02-29') AS truncated_year,
       date_trunc('month', TIMESTAMP '2024-02-29 12:34:56.123456') AS truncated_month,
       date_trunc('hour', TIMESTAMP '2024-02-29 12:34:56.123456') AS truncated_hour,
       CASE WHEN true THEN DATE '2024-02-29'
            ELSE TIMESTAMP '2024-03-01 01:02:03' END AS date_timestamp_case,
       coalesce(CAST(NULL AS DATE), TIMESTAMP '2024-03-01 01:02:03')
           AS date_timestamp_coalesce,
       DATE '2024-02-29' = TIMESTAMP '2024-02-29 00:00:00'
           AS date_timestamp_comparison,
       nullif(DATE '2024-02-29', TIMESTAMP '2024-03-01 00:00:00')
           AS date_timestamp_nullif;
