SELECT count(*) AS output_rows,
       count(o.o_orderkey) AS matched_orders,
       count(l.l_orderkey) AS matched_lineitems
FROM (
  SELECT o_orderkey
  FROM read_parquet('__TPCH_ROOT__/orders/*.parquet')
) AS o
FULL JOIN (
  SELECT l_orderkey
  FROM read_parquet('__TPCH_ROOT__/lineitem/*.parquet')
) AS l
  ON o.o_orderkey = l.l_orderkey;
