CREATE OR ALTER VIEW silver.employee_dataframe AS

SELECT
b.[name],
b.surname,
s.[login],
s.age,
s.position,
s.gross_salary

FROM dbo.employee_csv b --bronze shortcut

JOIN silver.employee s -- silver shortcut
ON b.[login] = s.[login]