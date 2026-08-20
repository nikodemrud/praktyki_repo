IF EXISTS (
	SELECT * 
	FROM sys.external_tables 
	WHERE name = 'employee' AND schema_id = SCHEMA_ID('gold')
)
	DROP EXTERNAL TABLE gold.employee

CREATE EXTERNAL TABLE gold.employee

WITH (
    LOCATION = 'gold/employee',
    DATA_SOURCE = bronze_employee,
    FILE_FORMAT = SynapseParquetFormat
)
AS
SELECT 
    [name], 
    surname, 
    [login],
    age, 
    position, 
    gross_salary,
    gross_salary * 0.75 AS net_salary
FROM silver.employee