SELECT nation,
       o_year,
       sum(amount) AS sum_profit
FROM (
    SELECT n.n_name AS nation,
           extract(year FROM o.o_orderdate) AS o_year,
           l.l_extendedprice * (1 - l.l_discount)
             - ps.ps_supplycost * l.l_quantity AS amount
    FROM read_parquet('__TPCH_ROOT__/part/*.parquet') AS p
    JOIN read_parquet('__TPCH_ROOT__/lineitem/*.parquet') AS l
      ON p.p_partkey = l.l_partkey
    JOIN read_parquet('__TPCH_ROOT__/supplier/*.parquet') AS s
      ON l.l_suppkey = s.s_suppkey
    JOIN read_parquet('__TPCH_ROOT__/partsupp/*.parquet') AS ps
      ON l.l_partkey = ps.ps_partkey
     AND l.l_suppkey = ps.ps_suppkey
    JOIN read_parquet('__TPCH_ROOT__/orders/*.parquet') AS o
      ON l.l_orderkey = o.o_orderkey
    JOIN read_parquet('__TPCH_ROOT__/nation/*.parquet') AS n
      ON s.s_nationkey = n.n_nationkey
    WHERE p.p_name LIKE '%green%'
) AS profit
GROUP BY nation, o_year
ORDER BY nation, o_year DESC;
