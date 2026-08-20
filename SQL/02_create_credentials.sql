-- Create Credentials
IF NOT EXISTS (SELECT 1 FROM sys.symmetric_keys WHERE name LIKE '%DatabaseMasterKey%')
    CREATE MASTER KEY;

IF NOT EXISTS (SELECT 1 FROM sys.database_scoped_credentials WHERE name = 'SynapseIdentity')
    CREATE DATABASE SCOPED CREDENTIAL SynapseIdentity
    WITH IDENTITY = 'Managed Identity';
