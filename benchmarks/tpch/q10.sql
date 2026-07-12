SELECT c.c_custkey,
       c.c_name,
       sum(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
       c.c_acctbal,
       n.n_name,
       c.c_address,
       c.c_phone,
       c.c_comment
FROM read_parquet('__TPCH_ROOT__/customer/*.parquet') AS c
JOIN read_parquet('__TPCH_ROOT__/orders/*.parquet') AS o
  ON c.c_custkey = o.o_custkey
JOIN read_parquet('__TPCH_ROOT__/lineitem/*.parquet') AS l
  ON o.o_orderkey = l.l_orderkey
JOIN read_parquet('__TPCH_ROOT__/nation/*.parquet') AS n
  ON c.c_nationkey = n.n_nationkey
WHERE o.o_orderdate >= DATE '1993-10-01'
  AND o.o_orderdate < DATE '1994-01-01'
  AND l.l_returnflag = 'R'
GROUP BY c.c_custkey, c.c_name, c.c_acctbal, c.c_phone,
         n.n_name, c.c_address, c.c_comment
ORDER BY revenue DESC
LIMIT 20;
