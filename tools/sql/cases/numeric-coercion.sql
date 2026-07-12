SELECT CAST(2.50 AS DECIMAL(10, 2)) * CAST(4 AS DOUBLE) AS product,
       CAST(10 AS DECIMAL(10, 2)) / CAST(7 AS DECIMAL(2, 1)) AS quotient,
       CAST(2.50 AS DECIMAL(10, 2)) < CAST(3 AS DOUBLE) AS compared,
       CASE WHEN true
            THEN CAST(2.50 AS DECIMAL(10, 2))
            ELSE CAST(3 AS DOUBLE)
       END AS case_value,
       CAST(2.50 AS DECIMAL(10, 2)) >
           avg(CAST(x AS DECIMAL(10, 2))) AS avg_compared
FROM read_csv('__DATA__', header = true);
