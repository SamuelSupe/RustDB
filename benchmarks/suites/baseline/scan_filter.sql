SELECT sum(l_extendedprice * l_discount) AS revenue
FROM read_parquet('__TPCH_ROOT__/lineitem/*.parquet') AS lineitem
WHERE l_shipdate >= DATE '1994-01-01'
  AND l_shipdate < DATE '1995-01-01'
  AND l_discount BETWEEN 0.04 AND 0.06
  AND l_quantity < 24;
