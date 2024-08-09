{{
  config(
    cluster_by = ["retail_id","sku"]
  )
}}

WITH SCRAPING_DATA AS (
 SELECT scraped_at,scraped_at_date,sku,retail_id,product_name,product_url,offer_price,normal_price,card_price,loyalty_price,picture_url ,
    CASE WHEN brand is null THEN 'sin marca' ELSE lower(brand) END AS brand,rating,n_reviews,pos, 
  --   ROW_NUMBER() OVER (PARTITION BY sku,retail_id ORDER BY scraped_at DESC) as N,
     CURRENT_DATE('America/Santiago') AS process_date
            FROM {{ source('operacion','SCRAPINGS')}} 
             WHERE _TABLE_SUFFIX>= REPLACE( CAST(DATE_SUB(CURRENT_DATE('America/Santiago'), INTERVAL 30 DAY)  AS STRING),'-','' )
), MAX_DATA AS (
SELECT sku,retail_id, max(scraped_at) as scraped_at
from SCRAPING_DATA
group by sku,retail_id
), TODAY_DATA AS (
 SELECT retail_id, CAST(max(scraped_at) AS DATE) as scraped_at_date
 fROM MAX_DATA
)

SELECT SCRAPING_DATA.*,MAX_DATA.scraped_at_date  as retail_max_data,CASE WHEN TODAY_DATA.scraped_at_date=MAX_DATA.scraped_at_date THEN TRUE ELSE FALSE END AS flag_today,   GENERATE_UUID()  AS SKU_ID , 1 as N
FROM SCRAPING_DATA
JOIN MAX_DATA
ON MAX_DATA.sku=SCRAPING_DATA.sku AND 
 MAX_DATA.retail_id=SCRAPING_DATA.retail_id AND 
MAX_DATA.scraped_at=SCRAPING_DATA.scraped_at 
JOIN TODAY_DATA
ON TODAY_DATA.retail_id=SCRAPING_DATA.retail_id

