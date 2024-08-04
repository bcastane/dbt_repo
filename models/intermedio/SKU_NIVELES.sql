{{
  config(
    cluster_by = ["retail_id","sku"]
  )
}}

WITH LEVEL_DATA AS (
    SELECT DISTINCT retail_id,sku,nivel_1,nivel_2,nivel_3
        FROM  {{ source('operacion','SCRAPINGS')}} 
         WHERE _TABLE_SUFFIX>= REPLACE( CAST(DATE_SUB(CURRENT_DATE('America/Santiago'), INTERVAL 30 DAY)  AS STRING),'-','' )
         )

    SELECT *
    FROM LEVEL_DATA

    