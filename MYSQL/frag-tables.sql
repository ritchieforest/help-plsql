SELECT 
    table_schema AS `Database`,
    table_name AS `Table`,
    data_length AS `Data Length (Bytes)`,
    index_length AS `Index Length (Bytes)`,
    data_free AS `Free Space (Bytes)`,
    ROUND((data_free / (data_length + index_length)) * 100, 2) AS `Fragmentation (%)`
FROM 
    information_schema.tables
WHERE 
    table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
    AND data_free > 0
ORDER BY 
    `Fragmentation (%)` DESC;


-- Obtener todas las tablas con fragmentación
SELECT CONCAT('OPTIMIZE TABLE ', table_schema, '.', table_name, ';') AS optimize_cmd
FROM information_schema.tables
WHERE 
    table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
    AND data_free > 0;

-- Ejecutar manualmente las consultas resultantes
