SELECT SID.id as seller_id,SL.sku,SL.retail_id,seller_name FROM (select sku,retail_id,LOWER(seller) as seller_name 
                                 from  {{ source('comercial','SELLERS')}}  group by sku,retail_id,LOWER(seller)) as SL 
                                JOIN {{ source('comercial','SELLERS_ID')}}   SID
                                  ON LOWER(SL.seller_name) = LOWER(SID.seller) and SL.retail_id =SID.retail_id