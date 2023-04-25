{{ config(materialized='view') }}


SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY unique_id) AS row_num
FROM {{ ref('prep_data_match') }}