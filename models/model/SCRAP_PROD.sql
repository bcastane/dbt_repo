{{
  config(
    materialized = "table",
    cluster_by = "retail_id",
    partition_by = {
      "field": "process_date",
      "data_type": "date",
      "granularity": "day"
    },
  )
}}

    SELECT B.*,MT.product_id,V.visitas,parse_date("%Y%m%d",  CURRENT_DATE('America/Santiago')) AS process_date
    FROM (SELECT A.* , CASE WHEN CAST(SCRAPED_AT_DATE AS DATE)=CAST('@HOY' AS DATE) THEN TRUE ELSE FALSE END AS stock
        FROM (SELECT scraped_at,scraped_at_date,sku,product_name,
        normal_price,offer_price,card_price,loyalty_price,picture_url,retail_id,product_url,'' as size_list,1 as n_sizes,0 as reference_id,1 as N,brand, pos ,scraped_at_date as updated_at,scraped_at_date as created_at,rating,n_reviews,1 AS N_FILA
                 FROM {{ref("SKU_BASE")}} where CAST(scraped_at AS DATE)>= CAST('@date_min' AS DATE) ) AS A
        WHERE A.N_FILA=1) AS B
    JOIN  {{ source('operacion','MATCHS')  }}  AS MT
        ON B.sku=MT.sku AND B.retail_id=MT.retail_id
    LEFT JOIN  {{ source('operacion','VISITAS')  }}  AS V
     ON V.product_id=MT.product_id