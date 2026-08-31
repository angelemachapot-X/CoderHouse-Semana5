-- =========================================================================
-- CONSULTAS DE INTROSPECCIÓN Y ANÁLISIS EN AMAZON ATHENA (APACHE ICEBERG)
-- =========================================================================

-- 1. Consultar los datos procesados por Flink y persistidos en Iceberg
SELECT 
    sensor_id,
    ROUND(avg_temperature, 2) AS temperatura_promedio_celsius,
    from_unixtime(event_time_millis / 1000) AS fecha_hora_registro,
    event_time_millis
FROM "lakehouse_db"."sensor_events"
ORDER BY event_time_millis DESC
LIMIT 50;

-- 2. Inspeccionar el historial de Snapshots (Gobernanza y Time Travel)
SELECT 
    snapshot_id,
    parent_id,
    operation,
    committed_at,
    summary['added-records'] AS registros_agregados,
    summary['added-data-files'] AS archivos_parquet_agregados,
    manifest_list
FROM "lakehouse_db"."sensor_events$snapshots"
ORDER BY committed_at DESC;

-- 3. Inspeccionar los archivos Parquet físicos y métricas de Data Skipping
SELECT 
    file_path,
    file_format,
    record_count,
    file_size_in_bytes,
    column_sizes,
    value_counts,
    null_value_counts
FROM "lakehouse_db"."sensor_events$files";

-- 4. Inspeccionar las particiones y estadísticas de Partition Pruning
SELECT 
    partition,
    record_count,
    file_count
FROM "lakehouse_db"."sensor_events$partitions";

-- 5. Time Travel Query (Consultar el estado de la tabla en un snapshot anterior)
-- SELECT * FROM "lakehouse_db"."sensor_events" FOR VERSION AS OF <SNAPSHOT_ID>;
