 -- DROP TABLE users_cumulated
-- CREATE TABLE users_cumulated(
-- 	user_id TEXT,--no usamos integer porque es un numero muy grande
-- --The list of dates in the past where the user was active
-- 	dates_active DATE[],
-- --the current day for the user
-- 	date DATE,
-- 	PRIMARY KEY (user_id,date)
-- );
-- INSERT INTO
-- 	USERS_CUMULATED
-- WITH
-- 	YESTERDAY AS (
-- 		SELECT
-- 			*
-- 		FROM
-- 			USERS_CUMULATED
-- 		WHERE
-- 			DATE = DATE ('2023-01-30')
-- 	),
-- 	TODAY AS (
-- 		SELECT
-- 			CAST(USER_ID AS TEXT) AS USER_ID,
-- 			DATE (CAST(EVENT_TIME AS TIMESTAMP)) AS DATE_ACTIVE
-- 		FROM
-- 			EVENTS
-- 		WHERE
-- 			DATE (CAST(EVENT_TIME AS TIMESTAMP)) = DATE ('2023-01-31')
-- 			AND USER_ID IS NOT NULL
-- 		GROUP BY
-- 			USER_ID,
-- 			DATE (CAST(EVENT_TIME AS TIMESTAMP))
-- 	)
-- SELECT
-- 	COALESCE(T.USER_ID, Y.USER_ID) AS USER_ID,
-- 	CASE
-- 		WHEN Y.DATES_ACTIVE IS NULL THEN ARRAY[T.DATE_ACTIVE]
-- 		WHEN T.DATE_ACTIVE IS NULL THEN Y.DATES_ACTIVE
-- 		ELSE ARRAY[T.DATE_ACTIVE] || Y.DATES_ACTIVE
-- 	END AS DATES_ACTIVE,
-- 	COALESCE(T.DATE_ACTIVE, Y.DATE + INTERVAL '1 day') AS DATE
-- FROM
-- 	TODAY T
-- 	FULL OUTER JOIN YESTERDAY Y ON T.USER_ID = Y.USER_ID;

---Con la base de datos generada vamos a empezar a construir 
WITH users AS(
		SELECT * FROM users_cumulated
		WHERE date=DATE('2023-01-31')
),
	series AS(
		SELECT *
		FROM generate_series(DATE('2023-01-01'),DATE('2023-01-31'),INTERVAL '1 day') 
		as series_date
	),
	place_holder_ints AS(
		SELECT 
		CASE WHEN 
			dates_active @>ARRAY[DATE(series_date)]
			THEN POW(2,32-(date-DATE(series_date)))
			ELSE 0
			END as placeholder_int_value,
			*
			FROM users CROSS JOIN series
			-- WHERE user_id='444502572952128450'
	)
SELECT
	user_id,
	SUM(placeholder_int_value),
	CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)),
	BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)))>0 
	AS dim_is_montly_active,
	BIT_COUNT(CAST('11111110000000000000000000000000' AS BIT(32)) & CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) >0
	AS dim_is_weekly_active,
	BIT_COUNT(CAST('10000000000000000000000000000000' AS BIT(32)) & CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) >0
	AS dim_is_daily_active
	FROM place_holder_ints
	GROUP BY user_id
	
/*Buenas prácticas y eficiencia:
Uso de WITH (CTE): Las CTE (Common Table Expressions) son útiles para organizar la consulta en bloques lógicos y mejorar la legibilidad. Sin embargo, su uso excesivo puede generar una sobrecarga en términos de rendimiento si no se optimiza correctamente. En este caso, las CTE están bien para dividir la lógica de la consulta.

Operadores de combinación: Usar FULL OUTER JOIN es adecuado cuando queremos combinar registros de dos tablas que pueden no tener coincidencias en todas las claves. Sin embargo, es importante considerar si realmente necesitamos un FULL OUTER JOIN o si un LEFT JOIN o INNER JOIN sería más eficiente.

Índices: Asegúrate de tener índices adecuados en las columnas que se utilizan en las condiciones de los JOIN y WHERE (por ejemplo, user_id, date, event_time). Esto puede mejorar significativamente el rendimiento de las consultas.

Uso de COALESCE y CASE: Estas funciones ayudan a manejar valores nulos, pero debes tener cuidado de no abusar de ellas en consultas que puedan ser muy grandes, ya que aumentan la complejidad y el tiempo de ejecución.

Uso de ARRAY[] y operadores como @>: El uso de arrays y el operador @> (que verifica si un valor está contenido en un array) es adecuado para manejar datos de múltiples fechas activas. Sin embargo, es importante asegurarse de que los arrays no se vuelvan demasiado grandes, ya que podrían impactar la eficiencia. También es recomendable analizar el uso de ARRAY en base a las características de los datos.

Uso de BIT_COUNT: Las funciones de bitmanipulación son eficientes, pero deben usarse con precaución. Asegúrate de que el uso de bits en lugar de otras estructuras de datos (como rangos de fechas) sea la mejor opción para tu caso de uso.*/
			
		
