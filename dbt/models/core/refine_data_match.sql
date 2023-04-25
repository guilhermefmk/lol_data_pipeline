{{ config(materialized='table') }}

SELECT
  *,
  CASE
    WHEN row_num = 1 THEN 'nao'
    ELSE 'sim'
  END AS delete_case
FROM {{ ref('row_numbers') }}
