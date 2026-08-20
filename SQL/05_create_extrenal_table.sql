-- Create External Table
IF EXISTS (
	SELECT * 
	FROM sys.external_tables 
	WHERE name = 'employee_csv' AND schema_id = SCHEMA_ID('dbo')
)
	DROP EXTERNAL TABLE dbo.employee_csv

CREATE EXTERNAL TABLE dbo.employee_csv ( 
    [name] nvarchar(255),
    surname nvarchar(255),
    [login] nvarchar(255),
    age nvarchar(255),
    position nvarchar(255),
    grosssalary nvarchar(255)
)
WITH (
    LOCATION = 'employee/employee.csv',
    DATA_SOURCE = bronze_employee,
    FILE_FORMAT = SynapseCsvFormat_semicolon
)

CREATE or alter view dbo.v_viewemployee_csv AS 
select * FROM dbo.employee_csv 