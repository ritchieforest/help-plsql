SELECT TOP 10
    DB_NAME(qt.dbid) 'Base de Datos',
    OBJECT_NAME(qt.objectid,qt.dbid)AS 'Nombre Objeto',
    SUBSTRING(qt.text, (qs.statement_start_offset/2)+1,
    ((CASE qs.statement_end_offset
    WHEN -1 THEN DATALENGTH(qt.text)
    ELSE qs.statement_end_offset
    END - qs.statement_start_offset)/2)+1) AS 'Texto',
    qs.execution_count AS 'Veces ejecutado',
    qs.total_logical_reads AS 'Total lecturas lógicas',
    qs.last_logical_reads AS 'Lecturas lógicas del último proceso',
    qs.total_logical_writes AS 'Total escrituras lógicas',
    qs.last_logical_writes AS 'Escrituras lógicas del último proces',
    qs.total_worker_time AS 'Total tiempo CPU',
    qs.last_worker_time AS 'Tiempo CPU del último proceso',
    qs.min_worker_time AS 'Minimo tiempo CPU',
    qs.max_worker_time AS 'Maximo tiempo CPU',
    qs.total_elapsed_time/1000000 AS 'Total tiempo (en seg)',
    qs.last_elapsed_time/1000000 AS 'Tiempo del último proceso (en seg)',
    qs.min_elapsed_time/1000000 AS 'Tiempo mínimo (en seg)',
    qs.max_elapsed_time/1000000 AS 'Tiempo máximo (en seg)',
    qs.last_execution_time AS 'Ultima vez que se ejecutó',
    qp.query_plan AS 'Plan de ejecución'
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
--WHERE DB_NAME(qt.dbid) = 'NOMBRE_DE_BD'
ORDER BY qs.total_elapsed_time DESC



--Información general de databases, sessions y procesos

SELECT * FROM sys.dm_exec_sessions where login_name <> 'sa';

SELECT * FROM sys.sysprocesses where blocked > 0;

SELECT name, is_read_committed_snapshot_on FROM sys.databases;

--Información general de los posibles bloqueos en conexiones y peticiones

SELECT Blocker.text , Blocker.*, *
FROM sys.dm_exec_connections AS Conns
INNER JOIN sys.dm_exec_requests AS BlockedReqs
    ON Conns.session_id = BlockedReqs.blocking_session_id
INNER JOIN sys.dm_os_waiting_tasks AS w
    ON BlockedReqs.session_id = w.session_id
CROSS APPLY sys.dm_exec_sql_text(Conns.most_recent_sql_handle) AS Blocker

---KILL 69
--Información sobre transacciones activas SQL-server

SELECT  L.request_session_id AS SPID,
    DB_NAME(L.resource_database_id) AS DatabaseName,
    O.Name AS LockedObjectName,
    P.object_id AS LockedObjectId,
    L.resource_type AS LockedResource,
    L.request_mode AS LockType,
    ST.text AS SqlStatementText,       
    ES.login_name AS LoginName,
    ES.host_name AS HostName,
    TST.is_user_transaction as IsUserTransaction,
    AT.name as TransactionName,
    CN.auth_scheme as AuthenticationMethod
FROM    sys.dm_tran_locks L
    JOIN sys.partitions P ON P.hobt_id = L.resource_associated_entity_id
    JOIN sys.objects O ON O.object_id = P.object_id
    JOIN sys.dm_exec_sessions ES ON ES.session_id = L.request_session_id
    JOIN sys.dm_tran_session_transactions TST ON ES.session_id = TST.session_id
    JOIN sys.dm_tran_active_transactions AT ON TST.transaction_id = AT.transaction_id
    JOIN sys.dm_exec_connections CN ON CN.session_id = ES.session_id
    CROSS APPLY sys.dm_exec_sql_text(CN.most_recent_sql_handle) AS ST
WHERE   resource_database_id = db_id()
ORDER BY L.request_session_id
-



--Ultimas select ejecutadas sin necesidad de Profiler

SELECT TOP (50) * 
FROM(SELECT COALESCE(OBJECT_NAME(s2.objectid),'Ad-Hoc') AS ProcName,
 execution_count,s2.objectid,
 (SELECT TOP 1 SUBSTRING(s2.TEXT,statement_start_offset / 2+1 ,
 ( (CASE WHEN statement_end_offset = -1
 THEN (LEN(CONVERT(NVARCHAR(MAX),s2.TEXT)) * 2)
ELSE statement_end_offset END)- statement_start_offset) / 2+1)) AS sql_statement,
 last_execution_time
FROM sys.dm_exec_query_stats AS s1
CROSS APPLY sys.dm_exec_sql_text(sql_handle) AS s2 ) x
WHERE sql_statement NOT like 'SELECT TOP 50 * FROM(SELECT %'
ORDER BY last_execution_time DESC

--Ultimas 50 Select más costosas ejecutadas

SELECT TOP (50)
    q.text, s.total_elapsed_time, s.max_elapsed_time, s.min_elapsed_time,
    s.last_elapsed_time, s.execution_count, last_execution_time, *
FROM sys.dm_exec_query_stats as s
CROSS APPLY sys.dm_exec_sql_text(plan_handle) AS q
WHERE s.last_execution_time > DateAdd(mi , -1500 , GetDate()) -- solo las recientementes
AND text not like '%sys.%' -- eliminar consulta sys
ORDER BY s.total_elapsed_time DESC

--Las 100 Select ejecutadas que consumen más tiempo en segundos

SELECT TOP 100
  qs.total_elapsed_time / qs.execution_count / 1000000.0 AS average_seconds,
  qs.total_elapsed_time / 1000000.0 AS total_seconds,
  qs.execution_count,
  SUBSTRING ( qt.text,qs.statement_start_offset/2,
  ((CASE WHEN qs.statement_end_offset = -1
  THEN LEN(CONVERT(NVARCHAR(MAX), qt.text)) * 2
  ELSE qs.statement_end_offset
  END ) - qs.statement_start_offset) / 2 ) AS individual_query,
  o.name AS object_name,
  DB_NAME(qt.dbid) AS database_name
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) as qt
LEFT OUTER JOIN sys.objects o ON qt.objectid = o.object_id
WHERE qt.dbid = DB_ID()
ORDER BY average_seconds DESC;

--Las 25 Select ejecutadas que consumen más IO (logical reads/writes)

SELECT TOP 25
  (total_logical_reads+total_logical_writes) / qs.execution_count AS average_IO,
  (total_logical_reads+total_logical_writes) AS total_IO,
   qs.execution_count AS execution_count,
   SUBSTRING (qt.text,qs.statement_start_offset/2,
   ((CASE WHEN qs.statement_end_offset = -1
   THEN LEN(CONVERT(NVARCHAR(MAX), qt.text)) * 2
   ELSE qs.statement_end_offset END) - qs.statement_start_offset)/2) AS indivudual_query,
   o.name AS object_name,
   DB_NAME(qt.dbid) AS database_name
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) as qt
LEFT OUTER JOIN sys.objects o ON qt.objectid = o.object_id
WHERE qt.dbid = DB_ID()
ORDER BY average_IO DESC;

--Los 25 StoredProcs que consumen más IO (total_physical_reads)

SELECT TOP(25) p.name AS [SP Name],qs.total_physical_reads AS [TotalPhysicalReads],
qs.total_physical_reads/qs.execution_count AS [AvgPhysicalReads], qs.execution_count,
qs.total_logical_reads,qs.total_elapsed_time, qs.total_elapsed_time/qs.execution_count
AS [avg_elapsed_time], qs.cached_time
FROM sys.procedures AS p WITH (NOLOCK)
INNER JOIN sys.dm_exec_procedure_stats AS qs WITH (NOLOCK)
ON p.[object_id] = qs.[object_id]
WHERE qs.database_id = DB_ID()
AND qs.total_physical_reads > 0
ORDER BY qs.total_physical_reads DESC, qs.total_logical_reads DESC OPTION (RECOMPILE);

--Con esto veremos el uso de los indices en nuestra BBDD

SELECT OBJECT_NAME(S.[OBJECT_ID]) AS [OBJECT NAME], I.[NAME] AS [INDEX NAME], USER_SEEKS, USER_SCANS, USER_LOOKUPS, USER_UPDATES
FROM SYS.DM_DB_INDEX_USAGE_STATS AS S 
INNER JOIN SYS.INDEXES AS I 
ON I.[OBJECT_ID] = S.[OBJECT_ID] 
AND I.INDEX_ID = S.INDEX_ID
WHERE OBJECTPROPERTY(S.[OBJECT_ID],'IsUserTable') = 1
ORDER BY OBJECT_NAME(S.[OBJECT_ID])
--Con esto veremos la memoria RAM que consume cada BBDD

--Si lanzamos la query podemos ver el consumo concreto de una base de datos. Y si esta consulta la ejecutamos de forma regular podemos observar si el consumo fluctúa o no.

SELECT database_id, COUNT (*) * 8 / 1024 AS MB_EN_USO 
FROM sys.dm_os_buffer_descriptors
GROUP BY database_id ORDER BY COUNT (*) * 8 / 1024 DESC