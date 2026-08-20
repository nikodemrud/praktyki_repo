-- Create External Data Source
CREATE EXTERNAL DATA SOURCE [bronze_employee]
    WITH (
    LOCATION = 'abfss://source@dcdatahubsynapsessbdls.dfs.core.windows.net',   -- path to the storage account
    CREDENTIAL = [SynapseIdentity]
    )

-- additional data source
-- additional data source