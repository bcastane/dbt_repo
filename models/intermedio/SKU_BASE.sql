{{
  config(
    materialized = "table",
    cluster_by = ["retail_id","sku"]
  )
}}
WITH CURRENT_DATA AS (
    SELECT scraped_at,scraped_at_date,sku,retail_id,product_name,product_url,offer_price,normal_price,card_price,loyalty_price,picture_url ,
    CASE WHEN brand is null THEN 'sin marca' ELSE lower(brand) END AS brand,rating,n_reviews,pos, 
     ROW_NUMBER() OVER (PARTITION BY sku,retail_id ORDER BY scraped_at DESC) as N,
     CURRENT_DATE('America/Santiago') AS process_date
            FROM {{ source('operacion','SCRAPINGS')}} ),
            LAST_ROW AS ( SELECT *  EXCEPT(N)
            FROM CURRENT_DATA
        WHERE N=1 )--,
            --UPDATED_DATA AS (
            --SELECT CASE WHEN SB.SKU_ID IS NOT NULL THEN SB.SKU_ID ELSE GENERATE_UUID() END AS SKU_ID, LAST_ROW.*
            --FROM LAST_ROW 
            --LEFT JOIN  {{ source('intermedio','SKU_BASE')  }}  AS SB
            --ON LAST_ROW.SKU=SB.SKU AND LAST_ROW.RETAIL_ID=SB.RETAIL_ID)

        SELECT *, GENERATE_UUID()  AS SKU_ID
        FROM UPDATED_DATA