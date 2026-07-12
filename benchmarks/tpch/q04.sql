SELECT o.o_orderpriority,
       count(*) AS order_count
FROM read_parquet('__TPCH_ROOT__/orders/*.parquet') AS o
WHERE o.o_orderdate >= DATE '1993-07-01'
  AND o.o_orderdate < DATE '1993-10-01'
  AND EXISTS (
      SELECT *
      FROM read_parquet('__TPCH_ROOT__/lineitem/*.parquet') AS l
      WHERE l.l_orderkey = o.o_orderkey
        AND l.l_commitdate < l.l_receiptdate
  )
GROUP BY o.o_orderpriority
ORDER BY o.o_orderpriority;
