SELECT grp,
       count(DISTINCT val) AS distinct_values,
       count(DISTINCT text) AS distinct_text,
       sum(DISTINCT CAST(val AS DECIMAL(10, 2))) AS distinct_decimal_sum,
       avg(DISTINCT id) AS distinct_average_id,
       min(DISTINCT val) AS distinct_minimum,
       max(DISTINCT val) AS distinct_maximum
FROM read_csv('__NULL_DATA__', header = true)
GROUP BY grp
ORDER BY grp;
