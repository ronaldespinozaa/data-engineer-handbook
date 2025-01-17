-- Seleccionar todos los registros de player_seasons
SELECT * FROM player_seasons;

/* Crear un tipo con las dimensiones que cambian en el tiempo */
CREATE TYPE season_stats AS (season INTEGER, -- Temporada en la que se registran las estadísticas
    gp INTEGER, -- Juegos jugados
    pts REAL, -- Puntos por temporada
    reb REAL, -- Rebotes por temporada
    ast REAL  -- Asistencias por temporada
);

/* Crear una tabla con las columnas no repetitivas y la dimensión season_stats */
CREATE TABLE players (
    player_name TEXT, -- Nombre del jugador
    height TEXT, -- Altura del jugador
    college TEXT, -- Universidad del jugador
    country TEXT, -- País del jugador
    draft_year TEXT, -- Año en el que fue seleccionado
    draft_round TEXT, -- Ronda en la que fue seleccionado
    draft_number TEXT, -- Número de selección
    season_stats season_stats[], -- Estadísticas por temporada (array)
    current_season INTEGER, -- Temporada actual del jugador
    PRIMARY KEY (player_name, current_season) -- Clave primaria combinada
);

/* Insertar datos en players utilizando FULL OUTER JOIN */
WITH yesterday AS (
    SELECT * FROM players
    WHERE current_season = 1995 -- Filtrar datos de la temporada 1995
),
today AS (
    SELECT * FROM player_seasons
    WHERE season = 1996 -- Filtrar datos de la temporada 1996
)
INSERT INTO players
SELECT
    COALESCE(t.player_name, y.player_name) AS player_name, -- Combinar nombres de jugadores de ambas tablas
    COALESCE(t.height, y.height) AS height, -- Combinar alturas de jugadores
    COALESCE(t.college, y.college) AS college, -- Combinar universidades
    COALESCE(t.country, y.country) AS country, -- Combinar países
    COALESCE(t.draft_year, y.draft_year) AS draft_year, -- Combinar años de selección
    COALESCE(t.draft_round, y.draft_round) AS draft_round, -- Combinar rondas de selección
    COALESCE(t.draft_number, y.draft_number) AS draft_number, -- Combinar números de selección
    CASE
        WHEN y.season_stats IS NULL THEN ARRAY[ROW(
            t.season,
            t.gp,
            t.pts,
            t.reb,
            t.ast
        )::season_stats] -- Si no hay datos previos, crear un nuevo array con estadísticas
        WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[ROW(
            t.season,
            t.gp,
            t.pts,
            t.reb,
            t.ast
        )::season_stats] -- Si hay datos, agregar al array existente
        ELSE y.season_stats
    END AS season_stats,
    COALESCE(t.season, y.current_season + 1) AS current_season -- Actualizar la temporada actual
FROM today t
FULL OUTER JOIN yesterday y 
ON t.player_name = y.player_name; -- Unir ambas tablas por el nombre del jugador

-- Consultar jugadores específicos
SELECT * FROM players 
WHERE current_season = 2001 -- Filtrar por la temporada actual
AND player_name = 'Michael Jordan'; -- Filtrar por el nombre del jugador

-- Desanidar un array en una serie de filas
WITH unnested AS (
    SELECT 
        player_name,
        UNNEST(season_stats)::season_stats AS season_stats -- Desanidar estadísticas por temporada
    FROM players
    WHERE current_season = 2001
    AND player_name = 'Michael Jordan'
)
SELECT 
    player_name,
    (season_stats::season_stats).* -- Extraer todas las columnas de season_stats
FROM unnested;

-- Eliminar y recrear la tabla players con nuevas columnas
DROP TABLE players;

CREATE TYPE scoring_class AS ENUM ('star', 'good', 'average', 'bad'); -- Crear un tipo enumerado para clasificar jugadores

CREATE TABLE players (
    player_name TEXT, -- Nombre del jugador
    height TEXT, -- Altura del jugador
    college TEXT, -- Universidad del jugador
    country TEXT, -- País del jugador
    draft_year TEXT, -- Año en el que fue seleccionado
    draft_round TEXT, -- Ronda en la que fue seleccionado
    draft_number TEXT, -- Número de selección
    season_stats season_stats[], -- Estadísticas por temporada (array)
    scoring_class scoring_class, -- Clasificación del jugador según su puntuación
    years_since_last_season INTEGER, -- Años desde su última temporada
    current_season INTEGER, -- Temporada actual del jugador
    PRIMARY KEY (player_name, current_season) -- Clave primaria combinada
);

WITH yesterday AS (
    SELECT * FROM players
    WHERE current_season = 2000 -- Filtrar datos de la temporada 2000
),
today AS (
    SELECT * FROM player_seasons
    WHERE season = 2001 -- Filtrar datos de la temporada 2001
)
INSERT INTO players
SELECT
    COALESCE(t.player_name, y.player_name) AS player_name, -- Combinar nombres de jugadores de ambas tablas
    COALESCE(t.height, y.height) AS height, -- Combinar alturas de jugadores
    COALESCE(t.college, y.college) AS college, -- Combinar universidades
    COALESCE(t.country, y.country) AS country, -- Combinar países
    COALESCE(t.draft_year, y.draft_year) AS draft_year, -- Combinar años de selección
    COALESCE(t.draft_round, y.draft_round) AS draft_round, -- Combinar rondas de selección
    COALESCE(t.draft_number, y.draft_number) AS draft_number, -- Combinar números de selección
    CASE
        WHEN y.season_stats IS NULL THEN ARRAY[ROW(
            t.season,
            t.gp,
            t.pts,
            t.reb,
            t.ast
        )::season_stats] -- Si no hay datos previos, crear un nuevo array con estadísticas
        WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[ROW(
            t.season,
            t.gp,
            t.pts,
            t.reb,
            t.ast
        )::season_stats] -- Si hay datos, agregar al array existente
        ELSE y.season_stats
    END AS season_stats,
    CASE
        WHEN t.season IS NOT NULL THEN
            CASE
                WHEN t.pts > 20 THEN 'star' -- Clasificar como estrella si tiene más de 20 puntos
                WHEN t.pts > 15 THEN 'good' -- Clasificar como bueno si tiene entre 15 y 20 puntos
                WHEN t.pts > 10 THEN 'average' -- Clasificar como promedio si tiene entre 10 y 15 puntos
                ELSE 'bad' -- Clasificar como malo si tiene menos de 10 puntos
            END::scoring_class
        ELSE y.scoring_class
    END AS scoring_class,
    CASE
        WHEN t.season IS NOT NULL THEN 0 -- Reiniciar años desde última temporada si es nueva temporada
        ELSE y.years_since_last_season + 1 -- Incrementar años desde última temporada
    END AS years_since_last_season,
    COALESCE(t.season, y.current_season + 1) AS current_season -- Actualizar la temporada actual
FROM today t
FULL OUTER JOIN yesterday y 
ON t.player_name = y.player_name; -- Unir ambas tablas por el nombre del jugador

-- Consultar jugadores específicos con nuevas columnas
SELECT * FROM players 
WHERE current_season = 2001 -- Filtrar por la temporada actual
AND player_name = 'Michael Jordan'; -- Filtrar por el nombre del jugador

-- Calcular factor de mejora de puntos
SELECT 
    player_name,
    (season_stats[CARDINALITY(season_stats)]::season_stats).pts /  -- Puntos de la última temporada
    CASE 
        WHEN (season_stats[1]::season_stats).pts = 0 THEN 1  -- Evitar división por cero
        ELSE (season_stats[1]::season_stats).pts  -- Puntos de la primera temporada
    END AS improvement_factor -- Factor de mejora
FROM players
WHERE current_season = 2001; -- Filtrar por la temporada actual
