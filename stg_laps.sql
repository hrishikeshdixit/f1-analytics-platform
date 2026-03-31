WITH source AS (
    SELECT * FROM {{ source('raw', 'laps') }}
),

renamed AS (
    SELECT
        -- Identity
        Driver                  AS driver_code,
        DriverNumber            AS driver_number,
        Team                    AS team,

        -- Race context
        RaceName                AS race_name,
        RoundNumber             AS round_number,
        Year                    AS year,
        SessionType             AS session_type,

        -- Lap info
        LapNumber               AS lap_number,
        LapTime                 AS lap_time_seconds,
        IsPersonalBest          AS is_personal_best,

        -- Sector times
        Sector1Time             AS sector_1_seconds,
        Sector2Time             AS sector_2_seconds,
        Sector3Time             AS sector_3_seconds,

        -- Tyre info
        Compound                AS tyre_compound,
        TyreLife                AS tyre_age_laps,
        FreshTyre               AS is_fresh_tyre,

        -- Speed traps
        SpeedI1                 AS speed_trap_1,
        SpeedI2                 AS speed_trap_2,
        SpeedFL                 AS speed_finish_line,
        SpeedST                 AS speed_longest_straight

    FROM source
    WHERE LapTime IS NOT NULL
)

SELECT * FROM renamed