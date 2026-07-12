SELECT s.s_name,
       s.s_address
FROM read_parquet('__TPCH_ROOT__/supplier/*.parquet') AS s
JOIN read_parquet('__TPCH_ROOT__/nation/*.parquet') AS n
  ON s.s_nationkey = n.n_nationkey
WHERE n.n_name = 'CANADA'
  AND s.s_suppkey IN (
      SELECT ps.ps_suppkey
      FROM read_parquet('__TPCH_ROOT__/partsupp/*.parquet') AS ps
      WHERE ps.ps_partkey IN (
          SELECT p.p_partkey
          FROM read_parquet('__TPCH_ROOT__/part/*.parquet') AS p
          WHERE p.p_name LIKE 'forest%'
      )
        AND ps.ps_availqty > (
            SELECT 0.5 * sum(l.l_quantity)
            FROM read_parquet('__TPCH_ROOT__/lineitem/*.parquet') AS l
            WHERE l.l_partkey = ps.ps_partkey
              AND l.l_suppkey = ps.ps_suppkey
              AND l.l_shipdate >= DATE '1994-01-01'
              AND l.l_shipdate < DATE '1995-01-01'
        )
  )
ORDER BY s.s_name;
