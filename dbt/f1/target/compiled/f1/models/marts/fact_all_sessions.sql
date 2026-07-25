WITH race AS (
    SELECT
        driver_code,
        driver_number,
        team,
        race_name,
        round_number,
        year,
        session_type,
        lap_number,
        lap_time_seconds,
        is_personal_best,
        sector_1_seconds,
        sector_2_seconds,
        sector_3_seconds,
        tyre_compound,
        tyre_age_laps,
        is_fresh_tyre,
        speed_trap_1,
        speed_trap_2,
        speed_finish_line,
        speed_longest_straight,
        'Race'          AS session_category,
        ROUND(sector_1_seconds + sector_2_seconds + sector_3_seconds, 3)
            AS theoretical_best_lap,
        ROUND(lap_time_seconds - LAG(lap_time_seconds)
            OVER (PARTITION BY driver_code, race_name ORDER BY lap_number), 3)
            AS lap_time_delta,
        CASE
            WHEN COUNT(*) OVER (PARTITION BY driver_code, race_name) >=
                 MAX(lap_number) OVER (PARTITION BY race_name) * 0.75
            THEN TRUE
            ELSE FALSE
        END             AS is_race_representative
    FROM `f1-analytics-491120`.`transformed`.`stg_laps`
),

qualifying AS (
    SELECT
        driver_code,
        driver_number,
        team,
        race_name,
        round_number,
        year,
        session_type,
        lap_number,
        lap_time_seconds,
        is_personal_best,
        sector_1_seconds,
        sector_2_seconds,
        sector_3_seconds,
        tyre_compound,
        tyre_age_laps,
        CAST(NULL AS BOOL) AS is_fresh_tyre,
        speed_trap_1,
        speed_trap_2,
        speed_finish_line,
        speed_longest_straight,
        'Qualifying'    AS session_category,
        ROUND(sector_1_seconds + sector_2_seconds + sector_3_seconds, 3)
            AS theoretical_best_lap,
        NULL            AS lap_time_delta,
        FALSE           AS is_race_representative
    FROM `f1-analytics-491120`.`transformed`.`stg_laps_qualifying`
),

practice AS (
    SELECT
        driver_code,
        driver_number,
        team,
        race_name,
        round_number,
        year,
        session_type,
        lap_number,
        lap_time_seconds,
        is_personal_best,
        sector_1_seconds,
        sector_2_seconds,
        sector_3_seconds,
        tyre_compound,
        tyre_age_laps,
        CAST(NULL AS BOOL) AS is_fresh_tyre,
        speed_trap_1,
        speed_trap_2,
        speed_finish_line,
        speed_longest_straight,
        'Practice'      AS session_category,
        ROUND(sector_1_seconds + sector_2_seconds + sector_3_seconds, 3)
            AS theoretical_best_lap,
        ROUND(lap_time_seconds - LAG(lap_time_seconds)
            OVER (PARTITION BY driver_code, race_name, session_type ORDER BY lap_number), 3)
            AS lap_time_delta,
        FALSE           AS is_race_representative
    FROM `f1-analytics-491120`.`transformed`.`stg_laps_practice`
),

sprint AS (
    SELECT
        driver_code,
        driver_number,
        team,
        race_name,
        round_number,
        year,
        session_type,
        lap_number,
        lap_time_seconds,
        is_personal_best,
        sector_1_seconds,
        sector_2_seconds,
        sector_3_seconds,
        tyre_compound,
        tyre_age_laps,
        CAST(NULL AS BOOL) AS is_fresh_tyre,
        speed_trap_1,
        speed_trap_2,
        speed_finish_line,
        speed_longest_straight,
        'Sprint'        AS session_category,
        ROUND(sector_1_seconds + sector_2_seconds + sector_3_seconds, 3)
            AS theoretical_best_lap,
        ROUND(lap_time_seconds - LAG(lap_time_seconds)
            OVER (PARTITION BY driver_code, race_name ORDER BY lap_number), 3)
            AS lap_time_delta,
        CASE
            WHEN COUNT(*) OVER (PARTITION BY driver_code, race_name) >=
                 MAX(lap_number) OVER (PARTITION BY race_name) * 0.75
            THEN TRUE
            ELSE FALSE
        END             AS is_race_representative
    FROM `f1-analytics-491120`.`transformed`.`stg_laps_sprint`
),

combined AS (
    SELECT * FROM race
    UNION ALL
    SELECT * FROM qualifying
    UNION ALL
    SELECT * FROM practice
    UNION ALL
    SELECT * FROM sprint
)

SELECT * FROM combined