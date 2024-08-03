{{
  config(
    cluster_by = ["retail_id","sku"]
  )
}}

WITH LEVEL_DATA AS (
    SELECT DISTINCT retail_id,sku,nivel_1,nivel_2,nivel_3
        FROM  {{ source('operacion','SCRAPINGS')}} )

    SELECT *
    FROM LEVEL_DATA

    