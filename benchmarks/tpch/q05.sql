SELECT n.n_name,
       sum(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM read_parquet('__TPCH_ROOT__/customer/*.parquet') AS c
JOIN read_parquet('__TPCH_ROOT__/orders/*.parquet') AS o
  ON c.c_custkey = o.o_custkey
JOIN read_parquet('__TPCH_ROOT__/lineitem/*.parquet') AS l
  ON o.o_orderkey = l.l_orderkey
JOIN read_parquet('__TPCH_ROOT__/supplier/*.parquet') AS s
  ON l.l_suppkey = s.s_suppkey
 AND c.c_nationkey = s.s_nationkey
JOIN read_parquet('__TPCH_ROOT__/nation/*.parquet') AS n
  ON s.s_nationkey = n.n_nationkey
JOIN read_parquet('__TPCH_ROOT__/region/*.parquet') AS r
  ON n.n_regionkey = r.r_regionkey
WHERE r.r_name = 'ASIA'
  AND o.o_orderdate >= DATE '1994-01-01'
  AND o.o_orderdate < DATE '1995-01-01'
GROUP BY n.n_name
ORDER BY revenue DESC;
