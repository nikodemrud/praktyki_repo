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

IF NOT EXISTS ( SELECT 1 FROM sys.external_file_formats WHERE name = 'SynapseCsvFormat_semicolon' )  
    CREATE EXTERNAL FILE FORMAT [SynapseCsvFormat_semicolon] 
    WITH ( FORMAT_TYPE = DELIMITEDTEXT,
       FORMAT_OPTIONS(FIELD_TERMINATOR = ';', STRING_DELIMITER = '"', FIRST_ROW = 2) )