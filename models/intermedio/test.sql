SELECT *
FROM {{ source('bigquery','RETAILS')}} 
LIMIT 10