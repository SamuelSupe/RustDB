SELECT l_returnflag,
       l_linestatus,
       count(*) AS line_count,
       sum(l_quantity) AS quantity
FROM read_parquet('__TPCH_ROOT__/lineitem/*.parquet') AS lineitem
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;
