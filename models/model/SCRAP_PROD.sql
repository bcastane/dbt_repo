{{
  config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    cluster_by="retail_id",
     partition_by={
      "field": "process_date",
      "data_type": "date",
      "granularity": "day"
    },
     partition_expiration_days = 90
   
  )
}}

-- alias=add_date_if_underscore('SCRAP_PROD_')

SELECT B.*, MT.product_id, V.visitas
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
               process_date,
               is_today AS stock,
               1 AS N_FILA
        FROM {{ ref("SKU_BASE") }}
        WHERE CAST(scraped_at AS DATE) >= CURRENT_DATE('America/Santiago') - 30
    
) AS B
JOIN {{ source('operacion', 'MATCHS') }} AS MT
    ON B.sku = MT.sku AND B.retail_id = MT.retail_id
LEFT JOIN {{ source('operacion', 'VISITAS') }} AS V
    ON V.product_id = MT.product_id
