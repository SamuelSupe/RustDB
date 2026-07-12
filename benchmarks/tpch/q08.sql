SELECT o_year,
       sum(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END)
       / sum(volume) AS mkt_share
FROM (
    SELECT extract(year FROM o.o_orderdate) AS o_year,
           l.l_extendedprice * (1 - l.l_discount) AS volume,
           n2.n_name AS nation
    FROM read_parquet('__TPCH_ROOT__/part/*.parquet') AS p
    JOIN read_parquet('__TPCH_ROOT__/lineitem/*.parquet') AS l
      ON p.p_partkey = l.l_partkey
    JOIN read_parquet('__TPCH_ROOT__/supplier/*.parquet') AS s
      ON l.l_suppkey = s.s_suppkey
    JOIN read_parquet('__TPCH_ROOT__/orders/*.parquet') AS o
      ON l.l_orderkey = o.o_orderkey
    JOIN read_parquet('__TPCH_ROOT__/customer/*.parquet') AS c
      ON o.o_custkey = c.c_custkey
    JOIN read_parquet('__TPCH_ROOT__/nation/*.parquet') AS n1
      ON c.c_nationkey = n1.n_nationkey
    JOIN read_parquet('__TPCH_ROOT__/region/*.parquet') AS r
      ON n1.n_regionkey = r.r_regionkey
    JOIN read_parquet('__TPCH_ROOT__/nation/*.parquet') AS n2
      ON s.s_nationkey = n2.n_nationkey
    WHERE r.r_name = 'AMERICA'
      AND o.o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
      AND p.p_type = 'ECONOMY ANODIZED STEEL'
) AS all_nations
GROUP BY o_year
ORDER BY o_year;
