

{{
  config(

    alias= add_date_if_underscore('TEST_')
  )
}}


SELECT *
FROM {{ source('operacion','RETAILS')}} 
LIMIT 10