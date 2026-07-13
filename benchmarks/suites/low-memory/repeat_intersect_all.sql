SELECT count(*) AS repeated_rows
FROM (
  SELECT l_orderkey
  FROM read_parquet('__TPCH_ROOT__/lineitem/*.parquet')
  INTERSECT ALL
  SELECT l_orderkey
  FROM read_parquet('__TPCH_ROOT__/lineitem/*.parquet')
  WHERE l_linenumber <= 3
) AS repeated;
