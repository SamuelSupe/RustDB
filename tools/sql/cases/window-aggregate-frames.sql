SELECT id,
       grp,
       val,
       sum(val) OVER (ORDER BY grp) AS default_range_sum,
       sum(val) OVER (
           ORDER BY id
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       ) AS rows_sum,
       avg(val) OVER (
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       ) AS whole_average,
       min(val) OVER (
           PARTITION BY grp ORDER BY val IS NULL
       ) AS default_range_min,
       max(val) OVER (
           PARTITION BY grp ORDER BY val IS NULL
       ) AS default_range_max,
       min(val) OVER (
           PARTITION BY grp ORDER BY id
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       ) AS rows_min,
       max(val) OVER (
           PARTITION BY grp ORDER BY id
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       ) AS rows_max,
       min(val) OVER (
           PARTITION BY grp
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       ) AS whole_min,
       max(val) OVER (
           PARTITION BY grp
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       ) AS whole_max
FROM read_csv('__NULL_DATA__', header = true)
ORDER BY id;
