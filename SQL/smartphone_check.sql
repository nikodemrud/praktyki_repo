-- This is auto-generated code
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://dcdatahubsynapsessbdls.dfs.core.windows.net/silver/smartphone/',
        FORMAT = 'DELTA'
    ) AS [result]
