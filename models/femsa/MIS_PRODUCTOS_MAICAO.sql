
    CREATE TEMP FUNCTION LEAST_ARRAY(arr ANY TYPE) AS ((
        SELECT min(a) FROM UNNEST(arr) a WHERE a is not NULL
    ))
    
    CREATE TEMP FUNCTION GREAT_ARRAY(arr ANY TYPE) AS ((
        SELECT max(a) FROM UNNEST(arr) a WHERE a is not NULL
    ))
    CREATE TEMP FUNCTION PRICE_FORMAT(price ANY TYPE) AS ((
        CONCAT("$",REPLACE(
           FORMAT("%'.0f",CAST( price AS NUMERIC)),
           ',',
           '.'
         ))
        
        ))
    
    SELECT
    *,
    CASE WHEN stock=True and card_price=Precio_min  and (stock_preunic) THEN 1 ELSE 0 END AS Competitivo_num,
    CASE WHEN stock=True and (stock_preunic) THEN 1 ELSE 0 END AS Transp_num,
    CASE WHEN stock= False THEN 'MAIS' WHEN card_price=Precio_min THEN 'MAIMIN'  WHEN card_price=Precio_max THEN 'MAIMAX'  ELSE '' END AS MAI_COLOR,
    CASE WHEN stock_preunic= False THEN 'PREUS' WHEN card_preunic=Precio_min THEN 'PREUMIN'  WHEN card_preunic=Precio_max THEN 'PREUMAX'  ELSE '' END AS PEU_COLOR
    FROM (
        SELECT
        A.sku,
        A.retail_id,
        A.normal_price,
        A.offer_price,
        A.card_price,
        A.product_url,
        A.picture_url,
        A.stock,
        N.brand,
        N.product_name,
        N.nivel_1,
        N.nivel_2,
        N.nivel_3,
        B.sku as sku_preunic,
        B.stock as stock_preunic,

        B.product_url as url_preunic,

        B.normal_price as normal_preunic,
 
        B.offer_price as offer_preunic,
 
        B.card_price as card_preunic,
    
        PRICE_FORMAT(B.card_price) as card_format_preunic,
        PRICE_FORMAT(A.card_price) as card_format_price,
        GREAT_ARRAY([A.aux_price,B.aux_price]) as Precio_max,
        LEAST_ARRAY([A.aux_price,B.aux_price]) as Precio_min
        
        
        FROM 
        (SELECT *,
         CASE WHEN stock=False THEN NULL ELSE card_price END AS aux_price
         FROM 
       {{ref('SCRAP_PROD')}} 
        WHERE  process_date=CURRENT_DATE('America/Santiago')
        and retail_id= 33) AS A
        LEFT JOIN 
        (SELECT *,
         CASE WHEN stock=False THEN NULL ELSE card_price END AS aux_price,
         ROW_NUMBER() OVER ( PARTITION BY  CAST(product_id AS STRING) ORDER BY stock,offer_price ) AS ROWN
         FROM 
        {{ref('SCRAP_PROD')}} 
        WHERE   process_date=CURRENT_DATE('America/Santiago')  and  retail_id= 34) AS B
        ON A.product_id= B.product_id AND B.ROWN=1
        JOIN pasaporcaja.TEMP.NIVELES_SKU as N
        ON A.sku=N.sku AND A.retail_id= N.retail_id
    )