SELECT count(*) NOT IN (SELECT CAST(NULL AS BIGINT)) AS aggregate_not_in_null
FROM read_csv('__OUTER_DATA__', header = true) AS o;
