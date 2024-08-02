 WITH SL AS (

SELECT SID.id as seller_id,SL.sku,SL.retail_id,seller_name FROM (select sku,retail_id,LOWER(seller) as seller_name 
                                 from  pasaporcaja.COMERCIAL.SELLERS  group by sku,retail_id,LOWER(seller)) as SL 
                                JOIN pasaporcaja.COMERCIAL.SELLERS_ID  SID
                                  ON LOWER(SL.seller_name) = LOWER(SID.seller) and SL.retail_id =SID.retail_id

 )
 
 
 SELECT P.product_id,
                                 SL.seller_id,
                                 P.fecha,
                                 P.product_name,
                                 P.retail_id,
                                 R.retail_name,
                                 P.sku,
                                 P.product_url,
                                 SL.seller_name,
                                 PD.brand,
                                 N.nivel_0,
                                 PD.nivel_1,
                                 PD.nivel_2,
                                 P.stock,
                                 min(P.normal_price) as normal_price,
                                 min( P.offer_price) as offer_price,
                                 min(P.card_price) as card_price,
                                 CASE WHEN P.retail_id=1 AND seller_name LIKE '%paris%' THEN FALSE
                                 WHEN P.retail_id=2 and seller_name LIKE '%falabella%' THEN FALSE
                                 WHEN P.retail_id=3 and seller_name LIKE '%shop ecsa%' THEN FALSE
                                   WHEN P.retail_id in (7,10) THEN FALSE
                                   ELSE TRUE END IS_MARKETPLACE
                                    
                          FROM (SELECT product_id,sku,product_name,product_url,retail_id,visitas,stock, offer_price, normal_price, card_price , scraped_at_date as fecha_actualizacion, PARSE_DATE('%Y%m%d', _TABLE_SUFFIX)  as fecha 
                              FROM  {{ref('SCRAP_PROD')}} 
                                WHERE _TABLE_SUFFIX=REPLACE(CAST(CURRENT_DATE('America/Santiago') AS STRING),'-','') and retail_id in (1,2,3,7,10,12,20) ) as P
                          LEFT JOIN {{ ref('SKU_SELLER_ID') }} SL
                          ON SL.sku=P.sku and SL.retail_id=P.retail_id
                          JOIN  {{ source('operacion','RETAILS')}} as R
                          ON P.retail_id=R.retail_id
                          JOIN {{ source('comercial','PRODUCT_DETAILS')}} as PD
                          ON PD.product_id=P.product_id
                          JOIN{{ source('comercial','NIVEL_0')}} as N
                          ON N.nivel_1_id=PD.nivel_1_id
                          GROUP BY P.product_id,SL.seller_id, P.fecha,P.product_name,P.retail_id,P.sku,P.product_url,R.retail_name,SL.seller_name  , PD.brand, N.nivel_0,
                                 PD.nivel_1,
                                 PD.nivel_2, P.stock