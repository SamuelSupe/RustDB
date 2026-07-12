SELECT count(*) AS distinct_orderkeys
FROM (
  SELECT o_orderkey AS orderkey
  FROM read_parquet('__TPCH_ROOT__/orders/*.parquet')
  UNION
  SELECT l_orderkey AS orderkey
  FROM read_parquet('__TPCH_ROOT__/lineitem/*.parquet')
) AS distinct_keys;
