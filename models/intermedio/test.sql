SELECT *
FROM {{ source('operacion','RETAILS')}} 
LIMIT 10