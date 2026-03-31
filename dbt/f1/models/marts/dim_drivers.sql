WITH laps AS (
    SELECT * FROM {{ ref('stg_laps') }}
),

driver_stats AS (
    SELECT
        driver_code,
        driver_number,
        team,

        -- Performance stats
        COUNT(*)                                    AS total_laps,
        ROUND(MIN(lap_time_seconds), 3)             AS fastest_lap,
        ROUND(AVG(lap_time_seconds), 3)             AS avg_lap_time,
        ROUND(MIN(sector_1_seconds), 3)             AS best_sector_1,
        ROUND(MIN(sector_2_seconds), 3)             AS best_sector_2,
        ROUND(MIN(sector_3_seconds), 3)             AS best_sector_3,

        -- Tyre usage
        COUNTIF(tyre_compound = 'SOFT')             AS laps_on_soft,
        COUNTIF(tyre_compound = 'MEDIUM')           AS laps_on_medium,
        COUNTIF(tyre_compound = 'HARD')             AS laps_on_hard,

        -- Speed stats
        ROUND(MAX(speed_finish_line), 1)            AS top_speed_finish_line,
        ROUND(AVG(speed_longest_straight), 1)       AS avg_speed_longest_straight,

    CASE 
        WHEN COUNT(*) >= 30 
        THEN TRUE 
        ELSE FALSE 
    END AS is_race_representative

    FROM laps
    WHERE lap_time_seconds IS NOT NULL
    GROUP BY driver_code, driver_number, team
)

SELECT * FROM driver_stats
