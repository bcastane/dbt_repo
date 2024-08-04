
    SELECT 
    PD.product_id,
    PD.nivel_1,
    PD.nivel_2,
    PD.nivel_3,
    PD.brand,
    S.stock,
    S.sku,
    S.product_name,
    S.retail_id,
    R.retail_name,
    S.normal_price,
    S.offer_price,
    S.card_price,
    CASE WHEN PROP.EXISTE_RETAIL IS NULL THEN 1 ELSE 0 END AS NO_EN_RETAIL
    FROM 
    (SELECT *,
             CASE WHEN stock=False THEN NULL ELSE card_price END AS aux_price
             FROM    {{ref('SCRAP_PROD')}} 
        WHERE   process_date=CURRENT_DATE('America/Santiago') 
     AND retail_id  in (33,34)) AS S
    JOIN `pasaporcaja.COMERCIAL.PRODUCT_DETAILS` AS PD ON S.product_id=PD.product_id
    LEFT JOIN
    (SELECT distinct product_id , 1 AS EXISTE_RETAIL
          
             FROM    {{ref('SCRAP_PROD')}} 
        WHERE   process_date=CURRENT_DATE('America/Santiago')   AND retail_id  in (33)) AS  PROP
     ON PROP.product_id=S.product_id
     JOIN  `pasaporcaja.OPERACION.RETAILS` AS R
     ON R.retail_id= S.retail_id
