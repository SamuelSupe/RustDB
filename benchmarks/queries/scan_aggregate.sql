-- Replace the path with a generated TPC-H lineitem Parquet dataset.
SELECT l_returnflag, count(*) AS rows, sum(l_extendedprice) AS gross
FROM read_parquet('/data/tpch/lineitem/*.parquet')
WHERE l_shipdate >= DATE '1997-01-01'
GROUP BY l_returnflag
ORDER BY l_returnflag;
