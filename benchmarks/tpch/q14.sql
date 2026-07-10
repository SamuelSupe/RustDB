SELECT 100.00 *
       sum(CASE WHEN p_type LIKE 'PROMO%'
                THEN l_extendedprice * (1 - l_discount)
                ELSE 0 END)
       / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM read_parquet('__TPCH_ROOT__/lineitem/*.parquet') AS lineitem,
     read_parquet('__TPCH_ROOT__/part/*.parquet') AS part
WHERE l_partkey = p_partkey
  AND l_shipdate >= DATE '1995-09-01'
  AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH;
