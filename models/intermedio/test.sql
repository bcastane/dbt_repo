

{{
  config(

   
    alias=add_date_if_underscore('TEST_')
  )
}}


SELECT *, CURRENT_DATE('America/Santiago') as current_date_cl
FROM {{ source('operacion','RETAILS')}} 

LIMIT 10
