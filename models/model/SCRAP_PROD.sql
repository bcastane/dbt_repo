{{
  config(
    materialized='table',
    cluster_by="retail_id",
    alias=add_date_if_underscore('SCRAP_PROD_')
  )
}}

SELECT B.*, MT.product_id, V.visitas
FROM (
    SELECT A.*, 
           CASE 
               WHEN CAST(SCRAPED_AT_DATE AS DATE) = CURRENT_DATE('America/Santiago') THEN TRUE 
               ELSE FALSE 
           END AS stock
    FROM (
        SELECT scraped_at,
               scraped_at_date,
               sku,
               product_name,
               normal_price,
               offer_price,
               card_price,
               loyalty_price,
               picture_url,
               retail_id,
               product_url,
               '' AS size_list,
               1 AS n_sizes,
               0 AS reference_id,
               1 AS N,
               brand,
               pos,
               scraped_at_date AS updated_at,
               scraped_at_date AS created_at,
               rating,
               n_reviews,
               1 AS N_FILA
        FROM {{ ref("SKU_BASE") }}
        WHERE CAST(scraped_at AS DATE) >= CURRENT_DATE('America/Santiago') - 30
    ) AS A
    WHERE A.N_FILA = 1
) AS B
JOIN {{ source('operacion', 'MATCHS') }} AS MT
    ON B.sku = MT.sku AND B.retail_id = MT.retail_id
LEFT JOIN {{ source('operacion', 'VISITAS') }} AS V
    ON V.product_id = MT.product_id
