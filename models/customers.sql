WITH CTE AS (
    SELECT customer_id,
           ROW_NUMBER() OVER (PARTITION BY company_name, contact_name ORDER BY customer_id) AS linha
    FROM {{source("sources", "customers")}}),
REMOVED AS (
SELECT
    *
FROM    
    CTE
WHERE 
    linha = 1
)
SELECT 
    c.*
FROM    
    {{source("sources", "customers")}} c JOIN REMOVED r on (c.customer_id = r.customer_id)




    
