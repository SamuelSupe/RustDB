SELECT department, count(*) AS employees, sum(salary) AS payroll
FROM read_csv('/workspace/tests/fixtures/employees.csv', header = true)
GROUP BY department
ORDER BY department;
