-- Crear tabla para almacenar métricas de usuario mensuales en formato de array
-- Tabla ya definida, comentada para referencia
-- CREATE TABLE array_metrics(
--     user_id NUMERIC,
--     month_start DATE,
--     metric_name TEXT,
--     metric_array REAL[],
--     PRIMARY KEY(user_id, month_start, metric_name)
-- );

-- Eliminación de la tabla (comentar en caso de no ser necesario)
-- DROP TABLE array_metrics;

-- Inserción de datos en la tabla array_metrics
INSERT INTO array_metrics

-- Bloque para calcular agregados diarios
WITH daily_aggregate AS (
    SELECT 
        user_id,
        DATE(event_time) AS date,
        COUNT(1) AS num_site_hits -- Cuenta los eventos por usuario en una fecha específica
    FROM events
    WHERE 
        DATE(event_time) = DATE('2023-01-03') -- Filtra eventos solo para la fecha dada
        AND user_id IS NOT NULL               -- Excluye registros con user_id nulo
    GROUP BY 
        user_id, 
        DATE(event_time)
),

-- Bloque para obtener datos existentes del mes anterior
yesterday_array AS (
    SELECT *
    FROM array_metrics
    WHERE month_start = DATE('2023-01-01') -- Filtra registros del inicio del mes anterior
)

-- Selección e inserción combinada de datos nuevos y existentes
SELECT 
    COALESCE(da.user_id, ya.user_id) AS user_id, -- Combina IDs de usuarios de ambas tablas
    COALESCE(ya.month_start, DATE_TRUNC('month', da.date)) AS month_start, -- Obtiene el inicio del mes
    'site_hits' AS metric_name, -- Define el nombre de la métrica

    -- Crea o actualiza el array de métricas
    CASE 
        WHEN ya.metric_array IS NOT NULL THEN 
            ya.metric_array || ARRAY[COALESCE(da.num_site_hits, 0)] -- Agrega nuevos hits al array
        WHEN ya.metric_array IS NULL THEN 
            ARRAY_FILL(0, ARRAY[COALESCE(da.date - DATE(DATE_TRUNC('month', da.date)), 0)]) 
            || ARRAY[COALESCE(da.num_site_hits, 0)] -- Crea un array inicial con ceros y agrega hits
    END AS metric_array
FROM 
    daily_aggregate da
FULL OUTER JOIN 
    yesterday_array ya 
ON 
    da.user_id = ya.user_id

-- Manejo de conflictos en caso de claves duplicadas
ON CONFLICT (user_id, month_start, metric_name)
DO UPDATE SET 
    metric_array = EXCLUDED.metric_array;

-- Resumen de métricas acumuladas por mes
WITH agg AS (
    SELECT 
        metric_name,
        month_start,
        ARRAY[
            SUM(metric_array[1]), -- Suma los valores del primer día del mes
            SUM(metric_array[2]), -- Suma los valores del segundo día del mes
            SUM(metric_array[3])  -- Suma los valores del tercer día del mes
        ] AS summed_array
    FROM 
        array_metrics
    GROUP BY 
        metric_name, month_start
)

-- Expande el array resumido a registros individuales
SELECT 
    metric_name,
    month_start + CAST(CAST(index - 1 AS TEXT) || 'day' AS INTERVAL) AS date, -- Calcula la fecha
    elem AS value -- Valor correspondiente del array
FROM 
    agg 
CROSS JOIN UNNEST(agg.summed_array) WITH ORDINALITY AS a(elem, index);
