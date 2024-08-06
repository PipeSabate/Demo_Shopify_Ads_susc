SELECT 
    order_number, 
    COUNT(*) 
FROM 
    {{ ref('dev_stg_orders') }} 
GROUP BY 
    order_number 
HAVING 
    COUNT(*) > 1
