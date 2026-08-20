CREATE OR ALTER VIEW gold.employee_dataframe AS

SELECT
silver.employee.[name],
silver.employee.surname,
silver.employee.[login],
silver.employee.age,
silver.employee.position,
silver.employee.gross_salary,
gold.employee.net_salary

FROM silver.employee

JOIN gold.employee ON silver.employee.[login] = gold.employee.[login]