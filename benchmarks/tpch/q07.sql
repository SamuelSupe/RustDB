SELECT supp_nation,
       cust_nation,
       l_year,
       sum(volume) AS revenue
FROM (
    SELECT n1.n_name AS supp_nation,
           n2.n_name AS cust_nation,
           extract(year FROM l.l_shipdate) AS l_year,
           l.l_extendedprice * (1 - l.l_discount) AS volume
    FROM read_parquet('__TPCH_ROOT__/supplier/*.parquet') AS s
    JOIN read_parquet('__TPCH_ROOT__/lineitem/*.parquet') AS l
      ON s.s_suppkey = l.l_suppkey
    JOIN read_parquet('__TPCH_ROOT__/orders/*.parquet') AS o
      ON l.l_orderkey = o.o_orderkey
    JOIN read_parquet('__TPCH_ROOT__/customer/*.parquet') AS c
      ON o.o_custkey = c.c_custkey
    JOIN read_parquet('__TPCH_ROOT__/nation/*.parquet') AS n1
      ON s.s_nationkey = n1.n_nationkey
    JOIN read_parquet('__TPCH_ROOT__/nation/*.parquet') AS n2
      ON c.c_nationkey = n2.n_nationkey
    WHERE ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
        OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
      AND l.l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
) AS shipping
GROUP BY supp_nation, cust_nation, l_year
ORDER BY supp_nation, cust_nation, l_year;
