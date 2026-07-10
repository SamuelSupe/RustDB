SELECT l_orderkey,
       sum(l_extendedprice * (1 - l_discount)) AS revenue,
       o_orderdate, o_shippriority
FROM read_parquet('__TPCH_ROOT__/customer/*.parquet') AS customer,
     read_parquet('__TPCH_ROOT__/orders/*.parquet') AS orders,
     read_parquet('__TPCH_ROOT__/lineitem/*.parquet') AS lineitem
WHERE c_mktsegment = 'BUILDING'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < DATE '1995-03-15'
  AND l_shipdate > DATE '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10;
