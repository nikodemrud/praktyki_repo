-- This is auto-generated code
EXEC sp_describe_first_result_set
N'
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK ''https://dcdatahubsynapsessbdls.dfs.core.windows.net/source/employee/login_results/part-00000-575d14bd-5d75-4875-9616-5cc819b34d36-c000.csv'',
        FORMAT = ''CSV'',
        PARSER_VERSION = ''2.0''
    ) AS [result]
'
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