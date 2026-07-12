SELECT CASE WHEN false THEN (SELECT 1 / 0) ELSE 0 END AS dead_projection;
