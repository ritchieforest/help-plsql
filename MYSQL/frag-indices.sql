SELECT 
    table_schema AS `Database`,
    table_name AS `Table`,
    index_name AS `Index`,
    stat_value AS `Leaf Pages`,
    round((stat_value / data_length) * 100, 2) AS `Fragmentation (%)`
FROM 
    information_schema.innodb_index_stats
JOIN 
    information_schema.tables 
    ON innodb_index_stats.table_name = tables.table_name 
    AND innodb_index_stats.table_schema = tables.table_schema
WHERE 
    stat_name = 'size' 
    AND table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
ORDER BY 
    `Fragmentation (%)` DESC;





-- Obtener todas las tablas con índices fragmentados
SELECT CONCAT('OPTIMIZE TABLE ', table_schema, '.', table_name, ';') AS optimize_cmd
FROM information_schema.innodb_index_stats
JOIN 
    information_schema.tables 
    ON innodb_index_stats.table_name = tables.table_name 
    AND innodb_index_stats.table_schema = tables.table_schema
WHERE 
    stat_name = 'size' 
    AND table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
    AND ROUND((stat_value / data_length) * 100, 2) > 10 -- Fragmentación mayor al 10%
ORDER BY 
    `Fragmentation (%)` DESC;

-- Ejecutar manualmente las consultas resultantes
