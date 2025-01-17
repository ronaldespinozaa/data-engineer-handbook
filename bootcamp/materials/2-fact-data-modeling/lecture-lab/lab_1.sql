-- Validamos si hay duplicados, lo cual es común en tablas de logs.
-- Esto nos permite identificar registros duplicados basados en las claves primarias esperadas.
SELECT 
    game_id,
    team_id,
    player_id,
    COUNT(1)
FROM game_details
GROUP BY 1, 2, 3
HAVING COUNT(1) > 1;

-- Eliminamos duplicados utilizando ROW_NUMBER para mantener únicamente un registro por combinación de claves.
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id) AS row_num
    FROM game_details
)
SELECT * 
FROM deduped
WHERE row_num = 1;

-- Este conjunto de datos está claramente desnormalizado, ya que falta información como la fecha del juego (WHEN).
-- En el diseño de un modelo de datos tipo FACT, es necesario incluir atributos temporales y contextuales.
-- Buscamos esta información en la tabla `games` y la utilizamos para poblar la tabla `fct_game_details`.

INSERT INTO fct_game_details
WITH deduped AS (
    SELECT
        g.game_date_est,
        g.season,
        g.home_team_id,
        gd.*,
        ROW_NUMBER() OVER (PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) AS row_num
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
)
SELECT 
    game_date_est AS dim_game_date,
    season AS dim_season,
    team_id AS dim_team_id,
    player_id AS dim_player_id,
    player_name AS dim_player_name,
    start_position AS dim_start_position,
    team_id = home_team_id AS dim_is_playing_at_home,
    COALESCE(POSITION('DNP' IN comment), 0) > 0 AS dim_did_not_play,
    COALESCE(POSITION('DND' IN comment), 0) > 0 AS dim_did_not_dress,
    COALESCE(POSITION('NWT' IN comment), 0) > 0 AS dim_not_with_team,
    CAST(SPLIT_PART(min, ':', 1) AS REAL) + 
    CAST(SPLIT_PART(min, ':', 2) AS REAL) / 60 AS m_minutes,
    fgm AS m_fgm,
    fga AS m_fga,
    fg3m AS m_fg3m,
    fg3a AS m_fg3a,
    ftm AS m_ftm,
    fta AS m_fta,
    oreb AS m_oreb,
    dreb AS m_dreb,
    reb AS m_reb,
    ast AS m_ast,
    stl AS m_stl,
    blk AS m_blk,
    "TO" AS m_turnovers,
    pf AS m_pf,
    pts AS m_pts,
    plus_minus AS m_plus_minus
FROM deduped
WHERE row_num = 1;

-- Creamos la tabla de hechos (FACT) `fct_game_details` con las medidas (metrics) y dimensiones necesarias.
-- Este diseño incluye columnas de métricas como puntos, asistencias y rebotes, así como indicadores booleanos que describen el estado del jugador.
-- La tabla utiliza una clave primaria compuesta por las dimensiones más importantes: fecha del juego, equipo y jugador.

CREATE TABLE fct_game_details (
    dim_game_date DATE,
    dim_season INTEGER,
    dim_team_id INTEGER,
    dim_player_id INTEGER,
    dim_player_name TEXT,
    dim_start_position TEXT,
    dim_is_playing_at_home BOOLEAN,
    dim_did_not_play BOOLEAN,
    dim_did_not_dress BOOLEAN,
    dim_not_with_team BOOLEAN,
    m_minutes REAL,
    m_fgm INTEGER,
    m_fga INTEGER,
    m_fg3m INTEGER,
    m_fg3a INTEGER,
    m_ftm INTEGER,
    m_fta INTEGER,
    m_oreb INTEGER,
    m_dreb INTEGER,
    m_reb INTEGER,
    m_ast INTEGER,
    m_stl INTEGER,
    m_blk INTEGER,
    m_turnovers INTEGER,
    m_pf INTEGER,
    m_pts INTEGER,
    m_plus_minus INTEGER,
    PRIMARY KEY (dim_game_date, dim_team_id, dim_player_id)
);

-- Consulta agregada para analizar los datos del modelo FACT.
-- Calculamos el número de juegos por jugador y la suma total de puntos, además de un análisis de abandono basado en el indicador `dim_not_with_team`.
SELECT 
    dim_player_name,
    dim_is_playing_at_home,
    COUNT(1) AS num_games,
    SUM(m_pts) AS total_points,
    COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS bailed_num,
    CAST(COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS REAL) / COUNT(1) AS bail_pct
FROM fct_game_details
GROUP BY 1, 2
ORDER BY bail_pct DESC;

