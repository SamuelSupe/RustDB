SELECT o.o_custkey,
       count(*) AS order_count,
       sum(o.o_totalprice) AS total_price
FROM read_parquet('__TPCH_ROOT__/orders/*.parquet') AS o
GROUP BY o.o_custkey;
