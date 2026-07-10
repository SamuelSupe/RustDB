SELECT ps_partkey, sum(ps_supplycost * ps_availqty) AS value
FROM read_parquet('__TPCH_ROOT__/partsupp/*.parquet') AS partsupp,
     read_parquet('__TPCH_ROOT__/supplier/*.parquet') AS supplier,
     read_parquet('__TPCH_ROOT__/nation/*.parquet') AS nation
WHERE ps_suppkey = s_suppkey
  AND s_nationkey = n_nationkey
  AND n_name = 'GERMANY'
GROUP BY ps_partkey
HAVING sum(ps_supplycost * ps_availqty) > (
    SELECT sum(ps_supplycost * ps_availqty) * 0.0001
    FROM read_parquet('__TPCH_ROOT__/partsupp/*.parquet') AS partsupp,
         read_parquet('__TPCH_ROOT__/supplier/*.parquet') AS supplier,
         read_parquet('__TPCH_ROOT__/nation/*.parquet') AS nation
    WHERE ps_suppkey = s_suppkey
      AND s_nationkey = n_nationkey
      AND n_name = 'GERMANY'
)
ORDER BY value DESC;
