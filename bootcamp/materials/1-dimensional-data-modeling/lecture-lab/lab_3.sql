-- Definición de un tipo ENUM para los tipos de vértices.
-- IMPORTANCIA: Usar ENUM permite restringir los valores posibles, garantizando integridad de los datos 
-- y evitando errores como valores inválidos. Es más eficiente que usar una columna de texto para categorías pequeñas.
CREATE TYPE vertex_type AS ENUM('player', 'team', 'game');

-- Creación de la tabla para vértices.
-- EFICIENCIA: Definir una clave primaria compuesta por "identifier" y "type" garantiza que no habrá duplicados 
-- para combinaciones de identificador y tipo de vértice.
CREATE TABLE vertices (
    identifier TEXT,
    type vertex_type,
    properties JSON, -- Uso de JSON para almacenar datos flexibles y dinámicos.
    PRIMARY KEY (identifier, type)
);

-- Definición del tipo ENUM para los tipos de aristas.
-- IMPORTANCIA: Garantiza que las relaciones entre vértices solo puedan adoptar valores válidos y predefinidos.
CREATE TYPE edge_type AS ENUM(
    'plays_against',
    'shares_team',
    'plays_in',
    'plays_on'
);

-- Creación de la tabla de aristas.
-- IMPORTANCIA: Modelar relaciones explícitas permite construir un grafo y realizar análisis de redes.
-- EFICIENCIA: Las claves primarias compuestas garantizan unicidad en las conexiones.
CREATE TABLE edges (
    subject_identifier TEXT,
    subject_type vertex_type,
    object_identifier TEXT,
    object_type vertex_type,
    edge_type edge_type,
    properties JSON,
    PRIMARY KEY (
        subject_identifier,
        subject_type,
        object_identifier,
        object_type,
        edge_type
    )
);

-- Poblamiento de datos para la tabla de vértices con los juegos.
-- EFICIENCIA: Usar JSON reduce la necesidad de múltiples columnas y permite una estructura flexible.
INSERT INTO vertices
SELECT 
    game_id AS identifier,
    'game'::vertex_type AS type,
    json_build_object(
        'pts_home', pts_home,
        'pts_away', pts_away,
        'winning_team', CASE WHEN home_team_wins = 1 THEN home_team_id ELSE visitor_team_id END
    ) AS properties
FROM games;

-- Poblamiento de datos para jugadores.
-- EFICIENCIA: Agregar datos agregados como "number_of_games" y "total_points" reduce la necesidad de cálculos repetitivos en consultas posteriores.
WITH players_agg AS (
    SELECT 
        player_id AS identifier,
        MAX(player_name) AS player_name,
        COUNT(1) AS number_of_games,
        SUM(pts) AS total_points,
        ARRAY_AGG(DISTINCT team_id) AS teams
    FROM game_details
    GROUP BY player_id
)
INSERT INTO vertices
SELECT 
    identifier,
    'player'::vertex_type,
    json_build_object(
        'player_name', player_name,
        'number_of_games', number_of_games,
        'total_points', total_points,
        'teams', teams
    )
FROM players_agg;

-- Poblamiento de datos para equipos.
-- EFICIENCIA: La deduplicación garantiza que cada equipo tenga una única entrada, evitando registros redundantes.
WITH teams_deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY team_id) AS row_num
    FROM teams
)
INSERT INTO vertices
SELECT 
    team_id AS identifier,
    'team'::vertex_type AS type,
    json_build_object(
        'abbreviation', abbreviation,
        'nickname', nickname,
        'city', city,
        'arena', arena,
        'year_founded', yearfounded
    )
FROM teams_deduped
WHERE row_num = 1;

-- Verificación del poblamiento de vértices por tipo.
-- IMPORTANCIA: Comprobar el balance de los datos asegura una correcta distribución de las entidades.
SELECT type, COUNT(1)
FROM vertices
GROUP BY 1;

-- Poblamiento de datos para las relaciones "plays_in" entre jugadores y juegos.
-- EFICIENCIA: La deduplicación previa elimina registros redundantes, optimizando el volumen de datos insertados.
WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY player_id, game_id) AS row_num 
    FROM game_details
)
INSERT INTO edges
SELECT 
    player_id AS subject_identifier,
    'player'::vertex_type AS subject_type,
    game_id AS object_identifier,
    'game'::vertex_type AS object_type,
    'plays_in'::edge_type AS edge_type,
    json_build_object(
        'start_position', start_position,
        'pts', pts,
        'team_id', team_id,
        'team_abbreviation', team_abbreviation
    ) AS properties
FROM deduped
WHERE row_num = 1;

-- Consulta: Jugador con el puntaje máximo en un solo juego.
-- EFICIENCIA: La agregación con JOIN asegura que no se realicen cálculos innecesarios en grandes volúmenes de datos.
SELECT
    v.properties->>'player_name',
    MAX(CAST(e.properties->>'pts' AS INTEGER))
FROM vertices v
JOIN edges e
ON e.subject_identifier = v.identifier
AND e.subject_type = v.type
GROUP BY 1
ORDER BY 2 DESC;

-- Poblamiento de datos para las relaciones entre jugadores.
-- IMPORTANCIA: Definir las relaciones "shares_team" y "plays_against" permite análisis de redes como conexiones entre jugadores.
-- EFICIENCIA: Agrupar y deduplicar datos antes de insertarlos evita la redundancia y mejora el rendimiento.
WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY player_id, game_id) AS row_num 
    FROM game_details
),
filtered AS (
    SELECT * FROM deduped
    WHERE row_num = 1
),
aggregated AS (
    SELECT
        f1.player_id AS subject_player_id,
        f2.player_id AS object_player_id,
        CASE 
            WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'shares_team'::edge_type
            ELSE 'plays_against'::edge_type
        END AS edge_type,
        COUNT(1) AS num_games,
        SUM(f1.pts) AS subject_points,
        SUM(f2.pts) AS object_points
    FROM filtered f1
    JOIN filtered f2
    ON f1.game_id = f2.game_id
    AND f1.player_id <> f2.player_id
    WHERE f1.player_id > f2.player_id
    GROUP BY 
        f1.player_id, f2.player_id,
        CASE 
            WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'shares_team'::edge_type
            ELSE 'plays_against'::edge_type
        END
)
INSERT INTO edges
SELECT 
    subject_player_id AS subject_identifier,
    'player'::vertex_type AS subject_type,
    object_player_id AS object_identifier,
    'player'::vertex_type AS object_type,
    edge_type AS edge_type,
    json_build_object(
        'num_games', num_games,
        'subject_points', subject_points,
        'object_points', object_points
    )
FROM aggregated;

-- Consulta: Métricas de jugadores y relaciones.
-- IMPORTANCIA: Permite analizar el desempeño de jugadores en función de sus relaciones y juegos jugados.
SELECT
    v.properties->>'player_name',
    e.object_identifier,
    CAST(v.properties->>'number_of_games' AS REAL) /
    CASE 
        WHEN CAST(v.properties->>'total_points' AS REAL) = 0 THEN 1 
        ELSE CAST(v.properties->>'total_points' AS REAL) 
    END AS games_per_point,
    e.properties->>'subject_points',
    e.properties->>'num_games'
FROM vertices v
JOIN edges e
ON v.identifier = e.subject_identifier
AND v.type = e.subject_type
WHERE e.object_type = 'player'::vertex_type;
