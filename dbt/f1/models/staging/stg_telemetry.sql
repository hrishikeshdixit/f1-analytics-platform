WITH source AS (
    SELECT * FROM {{ source('raw', 'telemetry') }}
),

cleaned AS (
    SELECT
        Driver                      AS driver_code,
        RaceName                    AS race_name,
        RoundNumber                 AS round_number,
        Year                        AS year,
        SessionType                 AS session_type,
        CAST(LapNumber AS INT64)    AS lap_number,
        X                           AS pos_x,
        Y                           AS pos_y,
        COALESCE(Z, 0.0)            AS pos_z,
        Speed                       AS speed,
        COALESCE(Throttle, 0.0)     AS throttle,
        COALESCE(Brake, FALSE)      AS brake,
        COALESCE(nGear, 0)          AS gear,
        Distance                    AS distance,
        COALESCE(TRUE)  AS is_accurate
    FROM source
    WHERE X IS NOT NULL
    AND Y IS NOT NULL
    AND Speed IS NOT NULL
    AND Distance IS NOT NULL
    AND Driver IS NOT NULL
)

SELECT * FROM cleaned