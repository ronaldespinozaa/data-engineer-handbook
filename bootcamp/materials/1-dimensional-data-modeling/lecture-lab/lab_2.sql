-- Crear tabla players con atributos y clave primaria combinada
CREATE TABLE players (
    player_name TEXT, -- Nombre del jugador
    height TEXT, -- Altura del jugador
    college TEXT, -- Universidad del jugador
    country TEXT, -- País del jugador
    draft_year TEXT, -- Año de selección
    draft_round TEXT, -- Ronda de selección
    draft_number TEXT, -- Número de selección
    season_stats season_stats[], -- Estadísticas por temporada
    scoring_class scoring_class, -- Clasificación según rendimiento
    years_since_last_season INTEGER, -- Años desde última temporada
    current_season INTEGER, -- Temporada actual
    is_active BOOLEAN, -- Si el jugador está activo
    PRIMARY KEY (player_name, current_season) -- Clave primaria
);

-- Generar una inserción con datos agregados y cálculo de temporadas
INSERT INTO players
WITH years AS (
    SELECT * FROM GENERATE_SERIES(1996, 2022) AS season
), p AS (
    SELECT
        player_name,
        MIN(season) AS first_season
    FROM player_seasons
    GROUP BY player_name
), players_and_seasons AS (
    SELECT * 
    FROM p
    JOIN years y
        ON p.first_season <= y.season
), windowed AS (
    SELECT
        pas.player_name,
        pas.season,
        ARRAY_REMOVE(
            ARRAY_AGG(
                CASE WHEN ps.season IS NOT NULL THEN
                    ROW(
                        ps.season,
                        ps.gp,
                        ps.pts,
                        ps.reb,
                        ps.ast
                    )::season_stats
                END
            ) OVER (
                PARTITION BY pas.player_name
                ORDER BY COALESCE(pas.season, ps.season)
            ),
            NULL
        ) AS seasons
    FROM players_and_seasons pas
    LEFT JOIN player_seasons ps
        ON pas.player_name = ps.player_name
        AND pas.season = ps.season
    ORDER BY pas.player_name, pas.season
), static AS (
    SELECT
        player_name,
        MAX(height) AS height,
        MAX(college) AS college,
        MAX(country) AS country,
        MAX(draft_year) AS draft_year,
        MAX(draft_round) AS draft_round,
        MAX(draft_number) AS draft_number
    FROM player_seasons
    GROUP BY player_name
)
SELECT
    w.player_name,
    s.height,
    s.college,
    s.country,
    s.draft_year,
    s.draft_round,
    s.draft_number,
    seasons AS season_stats,
    CASE
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 20 THEN 'star'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 15 THEN 'good'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 10 THEN 'average'
        ELSE 'bad'
    END::scoring_class AS scoring_class,
    w.season - (seasons[CARDINALITY(seasons)]::season_stats).season AS years_since_last_active,
    w.season,
    (seasons[CARDINALITY(seasons)]::season_stats).season = season AS is_active
FROM windowed w
JOIN static s
    ON w.player_name = s.player_name;

-- Crear tabla players_scd con dimensiones de SCD (Slowly Changing Dimensions)
CREATE TABLE players_scd (
    player_name TEXT,
    scoring_class SCORING_CLASS,
    is_active BOOLEAN,
    start_season INTEGER,
    end_season INTEGER,
    current_season INTEGER,
    PRIMARY KEY (player_name, start_season)
);

-- Calcular cambios usando ventanas para agrupar por jugador y calcular rachas
WITH with_previous AS (
    SELECT
        player_name,
        current_season,
        scoring_class,
        is_active,
        LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
        LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active
    FROM players
    WHERE current_season <= 2021 -- Analizamos hasta la temporada 2021
), with_indicators AS (
    SELECT *,
        CASE
            WHEN scoring_class <> previous_scoring_class THEN 1
            WHEN is_active <> previous_is_active THEN 1
            ELSE 0
        END AS change_indicator
    FROM with_previous
), with_streaks AS (
    SELECT *,
        SUM(change_indicator) OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
    FROM with_indicators
)
SELECT
    player_name,
    scoring_class,
    is_active,
    MIN(current_season) AS start_season,
    MAX(current_season) AS end_season,
    2021 AS current_season
FROM with_streaks
GROUP BY player_name, streak_identifier, scoring_class, is_active;

-- Selección de los registros de la última temporada desde la tabla SCD
WITH last_season_scd AS (
    SELECT * 
    FROM players_scd
    WHERE current_season = 2021
      AND end_season = 2021
),

-- Selección de los registros históricos hasta la temporada 2021
historical_scd AS (
    SELECT 
        player_name,
        scoring_class,
        is_active,
        start_season,
        end_season
    FROM players_scd
    WHERE current_season = 2021
      AND end_season < 2021
),

-- Obtención de los datos de la temporada actual (2022)
this_season_data AS (
    SELECT * 
    FROM players
    WHERE current_season = 2022
),

-- Identificación de registros que no han cambiado entre temporadas
unchanged_records AS (
    SELECT 
        ts.player_name,
        ts.scoring_class,
        ts.is_active,
        ls.start_season,
        ls.current_season AS end_season
    FROM this_season_data ts
    JOIN last_season_scd ls
      ON ls.player_name = ts.player_name
    WHERE ts.scoring_class = ls.scoring_class
      AND ts.is_active = ls.is_active
),

-- Identificación de registros que han cambiado entre temporadas
changed_records AS (
    SELECT 
        ts.player_name,
        UNNEST(ARRAY[
            ROW(
                ls.scoring_class,
                ls.is_active,
                ls.start_season,
                ls.end_season
            )::scd_type,
            ROW(
                ts.scoring_class,
                ts.is_active,
                ts.current_season,
                ts.current_season
            )::scd_type
        ]) AS records
    FROM this_season_data ts
    LEFT JOIN last_season_scd ls
      ON ls.player_name = ts.player_name
    WHERE ts.scoring_class <> ls.scoring_class
       OR ts.is_active <> ls.is_active
),

-- Desanidar los registros modificados para tenerlos en un formato tabular
unnested_changed_records AS (
    SELECT 
        player_name,
        (records::scd_type).scoring_class,
        (records::scd_type).is_active,
        (records::scd_type).start_season,
        (records::scd_type).end_season
    FROM changed_records
),

-- Identificación de nuevos registros en la temporada actual
new_records AS (
    SELECT 
        ts.player_name,
        ts.scoring_class,
        ts.is_active,
        ts.current_season AS start_season,
        ts.current_season AS end_season
    FROM this_season_data ts
    LEFT JOIN last_season_scd ls
      ON ts.player_name = ls.player_name
    WHERE ls.player_name IS NULL
)

-- Combinación de los datos históricos, sin cambios, cambiados y nuevos en un único resultado
SELECT * FROM historical_scd
UNION ALL
SELECT * FROM unchanged_records
UNION ALL
SELECT * FROM unnested_changed_records
UNION ALL
SELECT * FROM new_records;
