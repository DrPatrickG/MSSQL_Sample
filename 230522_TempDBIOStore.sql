--Set database to store stats
USE StatsWarehouse 
GO

-- Create the TempdbIOStats table if it doesn't exist
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'wh_TempdbIOStats')
BEGIN
    CREATE TABLE wh_TempdbIOStats (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        SessionID INT,
        Reads BIGINT,
        Writes BIGINT,
        LogicalReads BIGINT,
        TotalIO BIGINT,
        SQLText NVARCHAR(MAX),
        CaptureTime DATETIME DEFAULT GETDATE()
    );
END

--Set an arbitrary end date / time 
DECLARE @endDT datetime = dateadd(hh, 12, getdate()) 

-- Infinite loop
WHILE getdate() < @endDT 
BEGIN
    -- Insert query results into the TempdbIOStats table
    INSERT INTO wh_TempdbIOStats (SessionID, Reads, Writes, LogicalReads, TotalIO, SQLText)
    SELECT distinct
        t.session_id,
        t.reads,
        t.writes,
        t.logical_reads,
        (t.reads + t.writes) AS total_io,
        SUBSTRING(qt.text, (t.statement_start_offset / 2) + 1, (
                (CASE t.statement_end_offset
                        WHEN - 1
                            THEN DATALENGTH(qt.text)
                        ELSE t.statement_end_offset
                        END - t.statement_start_offset) / 2) + 1) AS sql_text
    FROM
        sys.dm_db_task_space_usage AS r
        INNER JOIN sys.dm_exec_requests AS t ON r.session_id = t.session_id
        CROSS APPLY sys.dm_exec_sql_text(t.sql_handle) AS qt;

    -- Wait for 5 minutes
    WAITFOR DELAY '00:05:00';
END;