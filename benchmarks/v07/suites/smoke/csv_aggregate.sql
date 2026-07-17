SELECT department,
       count(*) AS employee_count
FROM read_csv('/workspace/tests/fixtures/employees.csv', header = true)
GROUP BY department
ORDER BY department;
