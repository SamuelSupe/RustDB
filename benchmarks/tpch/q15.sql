WITH revenue AS (
    SELECT l.l_suppkey AS supplier_no,
           sum(l.l_extendedprice * (1 - l.l_discount)) AS total_revenue
    FROM read_parquet('__TPCH_ROOT__/lineitem/*.parquet') AS l
    WHERE l.l_shipdate >= DATE '1996-01-01'
      AND l.l_shipdate < DATE '1996-04-01'
    GROUP BY l.l_suppkey
)
SELECT s.s_suppkey,
       s.s_name,
       s.s_address,
       s.s_phone,
       r.total_revenue
FROM read_parquet('__TPCH_ROOT__/supplier/*.parquet') AS s
JOIN revenue AS r
  ON s.s_suppkey = r.supplier_no
WHERE r.total_revenue = (SELECT max(total_revenue) FROM revenue)
ORDER BY s.s_suppkey;
