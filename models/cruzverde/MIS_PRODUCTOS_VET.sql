
    CREATE TEMP FUNCTION LEAST_ARRAY(arr ANY TYPE) AS (
    (SELECT min(a) FROM UNNEST(arr) a WHERE a IS NOT NULL)
);

CREATE TEMP FUNCTION GREAT_ARRAY(arr ANY TYPE) AS (
    (SELECT max(a) FROM UNNEST(arr) a WHERE a IS NOT NULL)
);

CREATE TEMP FUNCTION PRICE_FORMAT(price ANY TYPE) AS (
    CONCAT("$", REPLACE(
        FORMAT("%'.0f", CAST(price AS NUMERIC)),
        ',',
        '.'
    ))
);

SELECT
    *,
    CASE
        WHEN stock = True AND card_price = Precio_min AND stock THEN 1
        ELSE 0
    END AS Competitivo_num,
    CASE
        WHEN stock = True AND (stock_salcobrand OR stock_ahumada or stock_superzoo or stock_pethappy or stock_clubperrosygatos or stock_laika) THEN 1
        ELSE 0
    END AS Transp_num,
    CASE
        WHEN stock = False THEN '29S'
        WHEN card_price = Precio_min THEN '29MIN'
        WHEN card_price = Precio_max THEN '29MAX'
        ELSE ''
    END AS COLOR_29,
    CASE
        WHEN stock_salcobrand = False THEN '30S'
        WHEN card_price_salcobrand = Precio_min THEN '30MIN'
        WHEN card_price_salcobrand = Precio_max THEN '30MAX'
        ELSE ''
    END AS COLOR_30,
    CASE
        WHEN stock_ahumada = False THEN '32S'
        WHEN card_price_ahumada = Precio_min THEN '32MIN'
        WHEN card_price_ahumada = Precio_max THEN '32MAX'
        ELSE ''
    END AS COLOR_32,
    CASE
        WHEN stock_superzoo = False THEN '35S'
        WHEN card_price_superzoo = Precio_min THEN '35MIN'
        WHEN card_price_superzoo = Precio_max THEN '35MAX'
        ELSE ''
    END AS COLOR_35,
    CASE
        WHEN stock_pethappy = False THEN '36S'
        WHEN card_price_pethappy = Precio_min THEN '36MIN'
        WHEN card_price_pethappy = Precio_max THEN '36MAX'
        ELSE ''
    END AS COLOR_36,
    CASE
        WHEN stock_clubperrosygatos = False THEN '37S'
        WHEN card_price_clubperrosygatos = Precio_min THEN '37MIN'
        WHEN card_price_clubperrosygatos = Precio_max THEN '37MAX'
        ELSE ''
    END AS COLOR_37,
    CASE
        WHEN stock_laika = False THEN '38S'
        WHEN card_price_laika = Precio_min THEN '38MIN'
        WHEN card_price_laika = Precio_max THEN '38MAX'
        ELSE ''
    END AS COLOR_38
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
        B.sku AS sku_salcobrand,
        B.stock AS stock_salcobrand,
        B.product_url AS url_salcobrand,
        B.normal_price AS normal_salcobrand,
        B.offer_price AS offer_salcobrand,
        B.card_price AS card_price_salcobrand,
        C.sku AS sku_ahumada,
        C.stock AS stock_ahumada,
        C.product_url AS url_ahumada,
        C.normal_price AS normal_ahumada,
        C.offer_price AS offer_ahumada,
        C.card_price AS card_price_ahumada,
        D.sku AS sku_superzoo,
        D.stock AS stock_superzoo,
        D.product_url AS url_superzoo,
        D.normal_price AS normal_superzoo,
        D.offer_price AS offer_superzoo,
        D.card_price AS card_price_superzoo,
        E.sku AS sku_pethappy,
        E.stock AS stock_pethappy,
        E.product_url AS url_pethappy,
        E.normal_price AS normal_pethappy,
        E.offer_price AS offer_pethappy,
        E.card_price AS card_price_pethappy,
        F.sku AS sku_clubperrosygatos,
        F.stock AS stock_clubperrosygatos,
        F.product_url AS url_clubperrosygatos,
        F.normal_price AS normal_clubperrosygatos,
        F.offer_price AS offer_clubperrosygatos,
        F.card_price AS card_price_clubperrosygatos,
        G.sku AS sku_laika,
        G.stock AS stock_laika,
        G.product_url AS url_laika,
        G.normal_price AS normal_laika,
        G.offer_price AS offer_laika,
        G.card_price AS card_price_laika,
        PRICE_FORMAT(A.card_price) AS card_format_price,
        PRICE_FORMAT(B.card_price) AS card_format_salcobrand,
        PRICE_FORMAT(C.card_price) AS card_format_ahumada,
        PRICE_FORMAT(D.card_price) AS card_format_superzoo,
        PRICE_FORMAT(E.card_price) AS card_format_pethappy,
        PRICE_FORMAT(F.card_price) AS card_format_clubperrosygatos,
        PRICE_FORMAT(G.card_price) AS card_format_laika,
        GREAT_ARRAY([A.aux_price, B.aux_price,C.aux_price,D.aux_price,E.aux_price,F.aux_price,G.aux_price]) AS Precio_max,
        LEAST_ARRAY([A.aux_price, B.aux_price,C.aux_price,D.aux_price,E.aux_price,F.aux_price,G.aux_price]) AS Precio_min
    FROM (
    SELECT AUX.*,
            CASE WHEN AUX.stock = False THEN NULL ELSE AUX.card_price END AS aux_price
        FROM (
            SELECT * FROM 
        {{ref('SCRAP_PROD')}} 
        WHERE _TABLE_SUFFIX=REPLACE(CAST(CURRENT_DATE('America/Santiago') AS STRING),'-','')  ) AS AUX
    JOIN `pasaporcaja.CRUZVERDE.VET_SEARCH` AS VET
    ON  AUX.sku=REPLACE(VET.sku ,".0","")  AND AUX.retail_id=VET.retail_id
    ) AS A
    LEFT JOIN (
        SELECT *,
            CASE WHEN stock = False THEN NULL ELSE card_price END AS aux_price,
            ROW_NUMBER() OVER (PARTITION BY CAST(product_id AS STRING) ORDER BY stock, offer_price) AS ROWN
        FROM {{ref('SCRAP_PROD')}} 
        WHERE _TABLE_SUFFIX=REPLACE(CAST(CURRENT_DATE('America/Santiago') AS STRING),'-','')  and retail_id = 30
    ) AS B
    ON A.product_id = B.product_id AND B.ROWN = 1
        LEFT JOIN (
        SELECT *,
            CASE WHEN stock = False THEN NULL ELSE card_price END AS aux_price,
            ROW_NUMBER() OVER (PARTITION BY CAST(product_id AS STRING) ORDER BY stock, offer_price) AS ROWN
        FROM {{ref('SCRAP_PROD')}} 
        WHERE _TABLE_SUFFIX=REPLACE(CAST(CURRENT_DATE('America/Santiago') AS STRING),'-','')  and retail_id = 32
    ) AS C
    ON A.product_id = C.product_id AND C.ROWN = 1
    LEFT JOIN (
        SELECT *,
            CASE WHEN stock = False THEN NULL ELSE card_price END AS aux_price,
            ROW_NUMBER() OVER (PARTITION BY CAST(product_id AS STRING) ORDER BY stock, offer_price) AS ROWN
        FROM {{ref('SCRAP_PROD')}} 
        WHERE _TABLE_SUFFIX=REPLACE(CAST(CURRENT_DATE('America/Santiago') AS STRING),'-','')  and  retail_id = 35
    ) AS D
    ON A.product_id = D.product_id AND D.ROWN = 1
    LEFT JOIN (
        SELECT *,
            CASE WHEN stock = False THEN NULL ELSE card_price END AS aux_price,
            ROW_NUMBER() OVER (PARTITION BY CAST(product_id AS STRING) ORDER BY stock, offer_price) AS ROWN
        FROM {{ref('SCRAP_PROD')}} 
        WHERE _TABLE_SUFFIX=REPLACE(CAST(CURRENT_DATE('America/Santiago') AS STRING),'-','')  and retail_id = 36
    ) AS E
    ON A.product_id = E.product_id AND E.ROWN = 1
    LEFT JOIN (
        SELECT *,
            CASE WHEN stock = False THEN NULL ELSE card_price END AS aux_price,
            ROW_NUMBER() OVER (PARTITION BY CAST(product_id AS STRING) ORDER BY stock, offer_price) AS ROWN
          FROM {{ref('SCRAP_PROD')}} 
        WHERE _TABLE_SUFFIX=REPLACE(CAST(CURRENT_DATE('America/Santiago') AS STRING),'-','')  and retail_id = 37
    ) AS F
    ON A.product_id = F.product_id AND F.ROWN = 1
    LEFT JOIN (
        SELECT *,
            CASE WHEN stock = False THEN NULL ELSE card_price END AS aux_price,
            ROW_NUMBER() OVER (PARTITION BY CAST(product_id AS STRING) ORDER BY stock, offer_price) AS ROWN
           FROM {{ref('SCRAP_PROD')}} 
        WHERE _TABLE_SUFFIX=REPLACE(CAST(CURRENT_DATE('America/Santiago') AS STRING),'-','') 
        and retail_id = 38
    ) AS G
    ON A.product_id = G.product_id AND G.ROWN = 1
    LEFT JOIN pasaporcaja.TEMP.NIVELES_SKU AS N
    ON A.sku = N.sku AND A.retail_id = N.retail_id
)