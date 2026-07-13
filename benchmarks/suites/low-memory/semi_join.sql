SELECT count(*) AS orders_with_lineitems
FROM (
  SELECT o_orderkey
  FROM read_parquet('__TPCH_ROOT__/orders/*.parquet')
) AS o
LEFT SEMI JOIN (
  SELECT l_orderkey
  FROM read_parquet('__TPCH_ROOT__/lineitem/*.parquet')
) AS l
  ON o.o_orderkey = l.l_orderkey;
