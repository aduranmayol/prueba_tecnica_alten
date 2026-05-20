CREATE OR REPLACE TABLE `project-1712e632-a062-4327-9c3.INTEGRATION.integration_prueba_tecnica` AS

SELECT 
  id,
  name,
  continent,
  flag_url,
  gold_medals,
  silver_medals,
  bronze_medals,
  total_medals,
  rank,
  rank_total_medals,
  CURRENT_TIMESTAMP() AS transformed_at
FROM (
  SELECT *,
         ROW_NUMBER() OVER(PARTITION BY id ORDER BY total_medals DESC) as rn
  FROM `project-1712e632-a062-4327-9c3.SANDBOX_ARNAU.olympic_games_countries`
)
WHERE rn = 1;
