
    CREATE TEMP FUNCTION LEAST_ARRAY(arr ANY TYPE) AS ((
        SELECT min(a) FROM UNNEST(arr) a WHERE a is not NULL
    ));
    
    CREATE TEMP FUNCTION GREAT_ARRAY(arr ANY TYPE) AS ((
        SELECT max(a) FROM UNNEST(arr) a WHERE a is not NULL
    ));
    CREATE TEMP FUNCTION PRICE_FORMAT(price ANY TYPE) AS ((
        CONCAT("$",REPLACE(
           FORMAT("%'.0f",CAST( price AS NUMERIC)),
           ',',
           '.'
         ))
        
        ));
    
    SELECT
    *,
    CASE WHEN stock=True and card_price=Precio_min  and (stock_salcobrand or stock_drsimi or stock_ahumada) THEN 1 ELSE 0 END AS Competitivo_num,
    CASE WHEN stock=True and (stock_salcobrand or stock_drsimi or stock_ahumada) THEN 1 ELSE 0 END AS Transp_num,
    CASE WHEN stock= False THEN 'RCRUZS' WHEN card_price=Precio_min THEN 'RCRUZMIN'  WHEN card_price=Precio_max THEN 'RCRUZMAX'  ELSE '' END AS RCRUZ_COLOR,
    CASE WHEN stock_salcobrand= False THEN 'RSALCOS' WHEN card_salcobrand=Precio_min THEN 'RSALCOMIN'  WHEN card_salcobrand=Precio_max THEN 'RSALCOMAX'  ELSE '' END AS RSALCO_COLOR,
    CASE WHEN stock_drsimi= False THEN 'RSIMIS' WHEN card_drsimi=Precio_min THEN 'RSIMIMIN'  WHEN card_drsimi=Precio_max THEN 'RSIMIMAX'  ELSE '' END AS RSIMI_COLOR,
    CASE WHEN stock_ahumada= False THEN 'RAHUS' WHEN card_ahumada=Precio_min THEN 'RAHUMIN'  WHEN card_ahumada=Precio_max THEN 'RAHUMAX'  ELSE '' END AS RAHU_COLOR
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
        B.sku as sku_salcobrand,
        C.sku as sku_drsimi,
        D.sku as sku_ahumada,
        B.stock as stock_salcobrand,
        C.stock as stock_drsimi,
        D.stock as stock_ahumada,
        B.product_url as url_salcobrand,
        C.product_url as url_drsimi,
        D.product_url as url_ahumada,
        B.normal_price as normal_salcobrand,
        C.normal_price as normal_drsimi,
        D.normal_price as normal_ahumada,   
        B.offer_price as offer_salcobrand,
        C.offer_price as offer_drsimi,
        D.offer_price as offer_ahumada,   
        B.card_price as card_salcobrand,
        C.card_price as card_drsimi,
        D.card_price as card_ahumada,  
        PRICE_FORMAT(B.card_price) as card_format_salcobrand,
        PRICE_FORMAT(C.card_price) as card_format_drsimi,
        PRICE_FORMAT(D.card_price) as card_format_ahumada,
        PRICE_FORMAT(A.card_price) as card_format_price,
        GREAT_ARRAY([A.aux_price,B.aux_price,C.aux_price,D.aux_price]) as Precio_max,
        LEAST_ARRAY([A.aux_price,B.aux_price,C.aux_price,D.aux_price]) as Precio_min
        
        
        FROM 
        (SELECT *,
         CASE WHEN stock=False THEN NULL ELSE card_price END AS aux_price
         FROM 
         {{ref('SCRAP_PROD')}} 
        WHERE _TABLE_SUFFIX=REPLACE(CAST(CURRENT_DATE('America/Santiago') AS STRING),'-','') and retail_id= 29) AS A
        LEFT JOIN (
        SELECT
        SKU_FEMSA,
        retail_id,
        MAX(IF(CADENA_ID = 30, SKU_CADENA, NULL)) AS SKU_RETAIL_30,
        MAX(IF(CADENA_ID = 31, SKU_CADENA, NULL)) AS SKU_RETAIL_31,
        MAX(IF(CADENA_ID = 32, SKU_CADENA, NULL)) AS SKU_RETAIL_32,
        MAX(IF(CADENA_ID = 34, SKU_CADENA, NULL)) AS SKU_RETAIL_34
        FROM
        {{ source('femsa','MATCHS_DASHBOARD')}}
        GROUP BY
        SKU_FEMSA,retail_id) AS M1
        ON A.retail_id=M1.retail_id AND A.sku=M1.SKU_FEMSA
        LEFT JOIN 
        (SELECT *,
         CASE WHEN stock=False THEN NULL ELSE card_price END AS aux_price,
         ROW_NUMBER() OVER ( PARTITION BY  CAST(product_id AS STRING) ORDER BY stock,offer_price ) AS ROWN
         FROM 
         {{ref('SCRAP_PROD')}} 
        WHERE _TABLE_SUFFIX=REPLACE(CAST(CURRENT_DATE('America/Santiago') AS STRING),'-','') and
          retail_id= 30) AS B
        ON B.sku=M1.SKU_RETAIL_30
       
        LEFT JOIN
        (SELECT *,
         CASE WHEN stock=False THEN NULL ELSE card_price END AS aux_price,
         ROW_NUMBER() OVER ( PARTITION BY  CAST(product_id AS STRING) ORDER BY stock,offer_price ) AS ROWN
         FROM 
          {{ref('SCRAP_PROD')}} 
        WHERE _TABLE_SUFFIX=REPLACE(CAST(CURRENT_DATE('America/Santiago') AS STRING),'-','') and
          retail_id= 31) AS C
        ON C.sku=M1.SKU_RETAIL_31
        LEFT JOIN
        (SELECT * ,
         CASE WHEN stock=False THEN NULL ELSE card_price END AS aux_price,
         ROW_NUMBER() OVER ( PARTITION BY  CAST(product_id AS STRING) ORDER BY stock,offer_price ) AS ROWN
        FROM 
         {{ref('SCRAP_PROD')}} 
        WHERE _TABLE_SUFFIX=REPLACE(CAST(CURRENT_DATE('America/Santiago') AS STRING),'-','') and
         retail_id= 32) AS D
        ON D.sku=M1.SKU_RETAIL_32
        JOIN pasaporcaja.TEMP.NIVELES_SKU as N
        ON A.sku=N.sku AND A.retail_id= N.retail_id
    )
    