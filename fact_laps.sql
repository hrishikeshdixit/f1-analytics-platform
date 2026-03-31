WITH stg AS (
    SELECT * FROM {{ ref('stg_laps') }}
),

final AS (
    SELECT
        -- Identity
        driver_code,
        driver_number,
        team,

        -- Race context
        race_name,
        round_number,
        year,

        -- Lap details
        lap_number,
        lap_time_seconds,
        is_personal_best,

        -- Sector times
        sector_1_seconds,
        sector_2_seconds,
        sector_3_seconds,

        -- Tyre strategy
        tyre_compound,
        tyre_age_laps,
        is_fresh_tyre,

        -- Speed traps
        speed_trap_1,
        speed_trap_2,
        speed_finish_line,
        speed_longest_straight,

        -- Calculated fields
        ROUND(sector_1_seconds + sector_2_seconds + sector_3_seconds, 3)
            AS theoretical_best_lap,

        ROUND(lap_time_seconds - LAG(lap_time_seconds)
            OVER (PARTITION BY driver_code ORDER BY lap_number), 3)
            AS lap_time_delta,
        
    CASE 
        WHEN COUNT(*) OVER (PARTITION BY driver_code, race_name) >= 30 
        THEN TRUE 
        ELSE FALSE 
    END AS is_race_representative

    FROM stg
)

SELECT * FROM final