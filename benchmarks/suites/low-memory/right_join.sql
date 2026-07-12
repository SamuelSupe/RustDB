SELECT count(*) AS output_rows,
       count(l.l_orderkey) AS matched_lineitems
FROM (
  SELECT l_orderkey
  FROM read_parquet('__TPCH_ROOT__/lineitem/*.parquet')
) AS l
RIGHT JOIN (
  SELECT o_orderkey
  FROM read_parquet('__TPCH_ROOT__/orders/*.parquet')
) AS o
  ON l.l_orderkey = o.o_orderkey;
