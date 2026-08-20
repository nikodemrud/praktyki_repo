IF EXISTS (
	SELECT * 
	FROM sys.external_tables 
	WHERE name = 'employee' AND schema_id = SCHEMA_ID('silver')
)
	DROP EXTERNAL TABLE silver.employee

CREATE EXTERNAL TABLE silver.employee

WITH (
    LOCATION = 'silver/employee',
    DATA_SOURCE = bronze_employee,
    FILE_FORMAT = SynapseParquetFormat
)
AS

SELECT 
    [name], 
    surname, 
    LOWER ([login]) AS [login],
    CAST (age AS INT) AS age, 
    position, 
    CAST(grosssalary AS INT) AS gross_salary
FROM dbo.employee_csv