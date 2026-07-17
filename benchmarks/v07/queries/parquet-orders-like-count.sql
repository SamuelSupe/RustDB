SELECT count(o_orderkey) AS remaining_orders
FROM read_parquet('data/tpch-sf1/orders/*.parquet')
WHERE o_comment NOT LIKE '%special%requests%';
