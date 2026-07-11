SELECT department,
       count(*) AS employee_count,
       sum(salary) AS payroll
FROM read_csv('__TPCH_ROOT__/employees.csv', header = true) AS employees
GROUP BY department
ORDER BY department;
