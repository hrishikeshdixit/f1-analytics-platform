CREATE OR REPLACE TABLE `f1-analytics-491120.raw.laps` AS
SELECT 
  CASE 
    -- Condition 1: Red Bull
    WHEN DRIVER IN ('VER', 'HAD') AND TEAM = '' AND ROUNDNUMBER = 7 AND SESSIONTYPE = 'Practice 1' 
    THEN 'Red Bull Racing'
    
    -- Condition 2: Mercedes
    WHEN DRIVER IN ('RUS', 'ANT', 'VES') AND TEAM = ''  AND ROUNDNUMBER = 7 AND SESSIONTYPE = 'Practice 1'
    THEN 'Mercedes'
    
    -- Condition 3: Ferrari
    WHEN DRIVER IN ('LEC', 'HAM', 'BEG') AND TEAM = '' AND ROUNDNUMBER = 7 AND SESSIONTYPE = 'Practice 1'
    THEN 'Ferrari'

    -- Condition 4: Alpine
    WHEN DRIVER IN ('GAS ', 'COL', 'ARO') AND TEAM = '' AND ROUNDNUMBER = 7 AND SESSIONTYPE = 'Practice 1'
    THEN 'Alpine'

    -- Condition 5: Aston Martin
    WHEN DRIVER IN ('ALO', 'STR') AND TEAM = ''  AND ROUNDNUMBER = 7 AND SESSIONTYPE = 'Practice 1'
    THEN 'Aston Martin'

    -- Condition 6: Audi
    WHEN DRIVER IN ('HUL', 'BOR') AND TEAM = '' AND ROUNDNUMBER = 7 AND SESSIONTYPE = 'Practice 1'
    THEN 'Audi'
    
    -- Condition 7: Cadillac
    WHEN DRIVER IN ('BOT', 'PER', 'HER') AND TEAM = ''  AND ROUNDNUMBER = 7 AND SESSIONTYPE = 'Practice 1'
    THEN 'Cadillac'

    -- Condition 8: Haas F1 Team
    WHEN DRIVER IN ('OCO', 'BEA', 'HIR') AND TEAM = '' AND ROUNDNUMBER = 7 AND SESSIONTYPE = 'Practice 1'
    THEN 'Haas F1 Team'
    
    -- Condition 9: McLaren
    WHEN DRIVER IN ('NOR', 'PIA', 'FOR') AND TEAM = '' AND ROUNDNUMBER = 7 AND SESSIONTYPE = 'Practice 1'
    THEN 'McLaren'

    -- Condition 10: Racing Bulls
    WHEN DRIVER IN ('LIN', 'LAW') AND TEAM = ''  AND ROUNDNUMBER = 7 AND SESSIONTYPE = 'Practice 1'
    THEN 'Racing Bulls'

    -- Condition 11: Williams
    WHEN DRIVER IN ('ALB', 'SAI', 'BRO') AND TEAM = '' AND ROUNDNUMBER = 7 AND SESSIONTYPE = 'Practice 1'
    THEN 'Williams'
    
    -- Fallback: Keep the original value if no rules match
    ELSE TEAM 
  END AS TEAM,
  
  * EXCEPT(TEAM)
FROM `f1-analytics-491120.raw.laps`;