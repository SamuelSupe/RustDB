SELECT sum(l.l_extendedprice) / 7.0 AS avg_yearly
FROM read_parquet('__TPCH_ROOT__/lineitem/*.parquet') AS l
JOIN read_parquet('__TPCH_ROOT__/part/*.parquet') AS p
  ON l.l_partkey = p.p_partkey
WHERE p.p_brand = 'Brand#23'
  AND p.p_container = 'MED BOX'
  AND l.l_quantity < (
      SELECT 0.2 * avg(l2.l_quantity)
      FROM read_parquet('__TPCH_ROOT__/lineitem/*.parquet') AS l2
      WHERE l2.l_partkey = p.p_partkey
  );
