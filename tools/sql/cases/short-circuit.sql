SELECT CASE WHEN false THEN 1 / 0 ELSE 0 END AS skipped_case,
       CASE WHEN CAST(NULL AS BOOLEAN) THEN 1 / 0 ELSE 1 END AS null_case,
       false AND (1 / 0 = 0) AS skipped_and,
       true OR (1 / 0 = 0) AS skipped_or,
       coalesce(7, 1 / 0) AS skipped_coalesce,
       nullif(CAST(NULL AS BIGINT), 1 / 0) AS skipped_nullif;
