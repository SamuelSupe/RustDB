SELECT TIMESTAMP(0) '2024-02-29 12:34:56.999999' AS precision_zero,
       TIMESTAMP(3) '2024-02-29 12:34:56.123999' AS precision_millis,
       TIMESTAMP(3) '1969-12-31 23:59:59.123456' AS precision_before_epoch,
       TIMESTAMP(3) '1969-12-31 23:59:59.8765' AS negative_half_tie;
