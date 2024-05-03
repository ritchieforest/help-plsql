---- ANALIZA LOS  INDICES DE LA TABLAS -----------------

SELECT OBJECT_NAME(OBJECT_ID), index_id,index_type_desc,index_level,
avg_fragmentation_in_percent,avg_page_space_used_in_percent,page_count
FROM sys.dm_db_index_physical_stats
(DB_ID(N'MHO'), NULL, NULL, NULL , 'SAMPLED')
ORDER BY avg_fragmentation_in_percent DESC



----------------  Reinderixa los indices de toda la base de datos   sp: sp_MSforeachtable  --------------------
 https://www.sqlshack.com/an-introduction-to-sp_msforeachtable-run-commands-iteratively-through-all-tables-in-a-database/



USE MHO
GO
EXEC sp_MSforeachtable @command1="print '?' DBCC DBREINDEX ('?', ' ', 80)"
GO
EXEC sp_updatestats
GO 
