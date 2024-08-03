SELECT 
    SID.id AS seller_id,
    SL.sku,
    SL.retail_id,
    SL.seller_name
FROM (
    SELECT 
        sku,
        retail_id,
        LOWER(seller) AS seller_name
    FROM 
        {{ source('comercial', 'SELLERS') }}
    GROUP BY 
        sku, retail_id, LOWER(seller)
) AS SL
JOIN 
    {{ source('comercial', 'SELLERS_ID') }} SID
ON 
    LOWER(SL.seller_name) = LOWER(SID.seller) 
    AND SL.retail_id = SID.retail_id
