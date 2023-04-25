CREATE OR REPLACE TABLE `lol-data-project-383712.lol.match_data_clean`
PARTITION BY DATE(gameCreation)
CLUSTER BY championName AS
SELECT * FROM `lol-data-project-383712.dbt_lol_project.refine_data_match` WHERE delete_case = 'nao' AND game_mode = 'CLASSIC';