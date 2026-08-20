-- Create Database
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'utils')
    CREATE DATABASE utils


-- Create Credentials
IF NOT EXISTS (SELECT 1 FROM sys.symmetric_keys WHERE name LIKE '%DatabaseMasterKey%')
    CREATE MASTER KEY;

IF NOT EXISTS (SELECT 1 FROM sys.database_scoped_credentials WHERE name = 'SynapseIdentity')
    CREATE DATABASE SCOPED CREDENTIAL SynapseIdentity
    WITH IDENTITY = 'Managed Identity';


-- Create External Data Source
CREATE EXTERNAL DATA SOURCE [utils_config]
    WITH (
    LOCATION = 'abfss://glo-insights-utils@dcsadthbdevweuconfig.dfs.core.windows.net',   -- path to the storage account
    CREDENTIAL = [SynapseIdentity]
    )

-- additional data source
-- additional data source


-- Create external File Format
IF NOT EXISTS ( SELECT 1 FROM sys.external_file_formats WHERE name = 'SynapseCsvFormat' )  
    CREATE EXTERNAL FILE FORMAT [SynapseCsvFormat] 
    WITH ( FORMAT_TYPE = DELIMITEDTEXT,
       FORMAT_OPTIONS(FIELD_TERMINATOR = ',', STRING_DELIMITER = '"', FIRST_ROW = 2) )

IF NOT EXISTS ( SELECT 1 FROM sys.external_file_formats WHERE name = 'SynapseDeltaFormat' )  
    CREATE EXTERNAL FILE FORMAT [SynapseDeltaFormat] 
    WITH ( FORMAT_TYPE = DELTA )

IF NOT EXISTS ( SELECT 1 FROM sys.external_file_formats WHERE name = 'SynapseParquetFormat' )  
    CREATE EXTERNAL FILE FORMAT [SynapseParquetFormat] 
    WITH ( FORMAT_TYPE = PARQUET )


-- Create External Table
CREATE EXTERNAL TABLE dbo.data_product_security_config ( 
    environment nvarchar(255),
    layer nvarchar(255),
    user_group nvarchar(255),
    data_product nvarchar(255),
    [schema] nvarchar(255),
    permission nvarchar(255)
)
WITH (
    LOCATION = 'SQL_configs/data_product_security_config.csv',
    DATA_SOURCE = utils_config,
    FILE_FORMAT = SynapseCsvFormat
)

-- Drop and Create External Table
IF EXISTS (
	SELECT * 
	FROM sys.external_tables 
	WHERE name = 'data_product_security_config' AND schema_id = SCHEMA_ID('dbo')
)
	DROP EXTERNAL TABLE dbo.data_product_security_config

CREATE EXTERNAL TABLE dbo.data_product_security_config ( 
    environment nvarchar(255),
    layer nvarchar(255),
    user_group nvarchar(255),
    data_product nvarchar(255),
    [schema] nvarchar(255),
    permission nvarchar(255)
)
WITH (
    LOCATION = 'SQL_configs/data_product_security_config.csv',
    DATA_SOURCE = utils_config,
    FILE_FORMAT = SynapseCsvFormat
)


-- Create Schema
IF NOT EXISTS (select * from sys.schemas WHERE name = 'my_schema')
    CREATE SCHEMA my_schema


-- Create or alter view
CREATE OR ALTER VIEW my_schema.my_sample_view AS
SELECT
    c1,
    c2
FROM
    my_schema.my_table
WHERE
    c2 = 'x'


-- Describe data from Delta
EXEC sp_describe_first_result_set
N'
SELECT
    *
FROM
    OPENROWSET(
        BULK ''https://dcsadthbdevweugld.dfs.core.windows.net/sapbwgold/delta/account_desc/'', 
        FORMAT = ''DELTA''
    ) AS [result]
'