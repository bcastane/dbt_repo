
WITH CURRENT_DATA AS (
    SELECT scraped_at,scraped_at_date,sku,retail_id,product_name,product_url,offer_price,normal_price,card_price,loyalty_price,picture_url ,
    CASE WHEN brand is null THEN 'sin marca' ELSE lower(brand) END AS brand,rating,n_reviews,pos, 
     ROW_NUMBER() OVER (PARTITION BY sku,retail_id ORDER BY scraped_at DESC) as N,
     CURRENT_DATE('America/Santiago') AS process_date
            FROM {{ source('operacion','SCRAPINGS')}} 
             WHERE _TABLE_SUFFIX>= REPLACE( CAST(DATE_SUB(CURRENT_DATE('America/Santiago'), INTERVAL 30 DAY)  AS STRING),'-','' )
             ),
            LAST_ROW AS ( SELECT *  EXCEPT(N)
            FROM CURRENT_DATA
        WHERE N=1 )

        SELECT *, GENERATE_UUID()  AS SKU_ID
        FROM LAST_ROW