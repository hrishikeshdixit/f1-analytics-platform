WITH source AS (
    SELECT * FROM `f1-analytics-491120`.`raw`.`laps`
),

renamed AS (
    SELECT
        Driver                  AS driver_code,
        DriverNumber            AS driver_number,
        Team                    AS team,
        RaceName                AS race_name,
        RoundNumber             AS round_number,
        Year                    AS year,
        SessionType             AS session_type,
        LapNumber               AS lap_number,
        LapTime                 AS lap_time_seconds,
        IsPersonalBest          AS is_personal_best,
        Sector1Time             AS sector_1_seconds,
        Sector2Time             AS sector_2_seconds,
        Sector3Time             AS sector_3_seconds,
        Compound                AS tyre_compound,
        TyreLife                AS tyre_age_laps,
        SpeedI1                 AS speed_trap_1,
        SpeedI2                 AS speed_trap_2,
        SpeedFL                 AS speed_finish_line,
        SpeedST                 AS speed_longest_straight,

        -- Gap to fastest lap in same session and race
        LapTime - MIN(LapTime) OVER (
            PARTITION BY RaceName, SessionType
        )                       AS gap_to_fastest

    FROM source
    WHERE LapTime IS NOT NULL
    AND SessionType IN ('Qualifying', 'Sprint Qualifying')
)

SELECT * FROM renamed