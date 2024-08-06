{# 
stg_orders es el primer layer de la tabla estandard de ordenes proveniente de Shopify. 
Aquí preparamos los datos para ser consumidos en los distintos layers. 

Tener en cuenta que:
Al crear o modificar una metrica , dimensión - atributo, se vera reflejado en todas las tablas dependientes de esta.

Reglas:
1. Aqui no hacemos merge con otras tablas
2. Se nombran las métricas con sentido de parametrización. Si se comienza por un idioma, se continua con el mismo.
3. Todo codigo que se pueda crear en stg, se crea. Esto para evitar sumar codigo en las otras capas.
#}

{{ config(
    materialized='ephemeral',
    schema='dev'
) }}



{# Orders tomara todas las columnas necesitadas de la tabla estandard #}

WITH orders AS (

SELECT 
    DATE(created_at) AS order_created_date,
    financial_status,
    DATE(closed_at) AS closed_date,
    DATE(cancelled_at) AS cancel_order_date,
    cancel_reason,
    confirmed,
    order_number,
    id AS order_id,
    customer:id::STRING AS customer_id,
    subtotal_price,
    total_discounts,
    TOTAL_SHIPPING_PRICE_SET:shop_money:amount::NUMBER AS shipping_cost,
    total_price, -- total price es la suma del precio base + shipping
    currency,
    fulfillment_status,
    checkout_id,
    source_name,
    email AS order_email,
    payment_gateway_names[0]::STRING AS payment_gateway, 
    app_id,
    refunds,
    device_id,
    total_tax,
    landing_site,
    referring_site,
    CASE 
        WHEN referring_site LIKE '%google%' THEN 'Search'
        WHEN referring_site LIKE '%facebook%' THEN 'Facebook'
        WHEN referring_site LIKE '%inst%' THEN 'Instagram'
        WHEN landing_site LIKE '%utm_source=facebook%' THEN 'Facebook'
        WHEN landing_site LIKE '%fbclid=%' THEN 'Facebook'
        WHEN landing_site LIKE '%utm_source=Klaviyo%' THEN 'Klaviyo'
        WHEN landing_site LIKE '%utm_medium=email%' THEN 'Klaviyo'
        ELSE 'Direct'
    END AS traffic_referrer,
    billing_address:province::STRING AS region,
    billing_address:province_code::STRING AS region_code,
    CASE
        WHEN region_code = 'AI' THEN 'CL-AI'
        WHEN region_code = 'AN' THEN 'CL-AN'
        WHEN region_code = 'AP' THEN 'CL-AP'
        WHEN region_code = 'AR' THEN 'CL-AR'
        WHEN region_code = 'AT' THEN 'CL-AT'
        WHEN region_code = 'BI' THEN 'CL-BI'
        WHEN region_code = 'CO' THEN 'CL-CO'
        WHEN region_code = 'LI' THEN 'CL-LI'
        WHEN region_code = 'LL' THEN 'CL-LL'
        WHEN region_code = 'LR' THEN 'CL-LR'
        WHEN region_code = 'MA' THEN 'CL-MA'
        WHEN region_code = 'ML' THEN 'CL-ML'
        WHEN region_code = 'NB' THEN 'CL-NB'
        WHEN region_code = 'RM' THEN 'CL-RM'
        WHEN region_code = 'TA' THEN 'CL-TA'
        WHEN region_code = 'VS' THEN 'CL-VS'
        ELSE 'Código desconocido'
    END AS iso_region_code

FROM "DEMO_DATABASE"."SCHEMA_DATABASE"."ORDERS"

),

-- Se crea ft_purchase para realizar seguimiento de las primeras compras
-- Join se realizara mediante first_order_id

ft_purchase AS (
    SELECT
        ft_purchase_order_number,
        customer_id_ft,
        order_id AS first_order_id,
        order_created_date AS first_purchase_date
    FROM (
        SELECT
            customer_id AS customer_id_ft,
            order_id,
            order_number AS ft_purchase_order_number,
            order_created_date,
            ROW_NUMBER() OVER (PARTITION BY customer_id_ft ORDER BY order_created_date ASC) AS rn
        FROM orders
        WHERE financial_status = 'paid'
    ) AS ranked_orders
    WHERE rn = 1
)
,

{# Solución Temporal  
Se crea count_ft_purchase para obtener el total de primeras compras por fecha
Join se realizara mediante first_purchase_date #}
count_ft_purchase AS (
    SELECT 
        COUNT(ft_purchase_order_number) AS ft_purchases_month_pack,
        first_purchase_date as ft_purchase_date
    FROM ft_purchase
    GROUP BY first_purchase_date
)
,
orders_complete as (

SELECT
o.*,
CASE WHEN f.ft_purchase_order_number = o.order_number THEN 1 ELSE 0 END is_ft_purchase,
f.first_purchase_date,
f.ft_purchase_order_number,
ft.ft_purchases_month_pack,
TO_CHAR(DATE_TRUNC('month', o.order_created_date), 'YYYY') as later_year,
TO_CHAR(DATE_TRUNC('month', o.order_created_date), 'MM') as later_month,
TO_CHAR(DATE_TRUNC('month', f.first_purchase_date), 'YYYY') as ft_year,
TO_CHAR(DATE_TRUNC('month', f.first_purchase_date), 'MM') as ft_month,
CASE WHEN later_year - ft_year > 0 then (later_year - ft_year) * 12 ELSE 0 
END diferencia_ano,
later_month - ft_month + diferencia_ano as transcurred_months
FROM orders o
LEFT JOIN ft_purchase f
ON o.customer_id = f.customer_id_ft
LEFT JOIN count_ft_purchase ft
ON f.first_purchase_date = ft.ft_purchase_date

),

ft_purchases_chanel AS (
    {# Contamos la cantidad de primeras compras que tiene cada canal agrupado por día #}
    SELECT 
        SUM(is_ft_purchase) AS sum_is_ft_purchase,
        COUNT(DISTINCT order_id) AS sum_orders,
        SUM(CASE WHEN traffic_referrer = 'Instagram'AND is_ft_purchase = '1' THEN 1 ELSE 0 END) AS sum_ft_orders_ig,
        SUM(CASE WHEN traffic_referrer = 'Facebook' AND is_ft_purchase = '1' THEN 1 ELSE 0 END) AS sum_ft_orders_fb,
        SUM(CASE WHEN traffic_referrer = 'Direct'   AND is_ft_purchase = '1' THEN 1 ELSE 0 END) AS sum_ft_orders_direct,
        SUM(CASE WHEN traffic_referrer = 'Search'   AND is_ft_purchase = '1' THEN 1 ELSE 0 END) AS sum_ft_orders_search,
        SUM(CASE WHEN traffic_referrer = 'Klaviyo'  AND is_ft_purchase = '1' THEN 1 ELSE 0 END) AS sum_ft_orders_klaviyo,
        order_created_date as order_create_date_chanell
        
    FROM orders_complete
    GROUP BY order_created_date
)


Select o.*,
c.sum_ft_orders_ig,
c.sum_ft_orders_fb,
c.sum_ft_orders_direct,
c.sum_ft_orders_search,
c.sum_ft_orders_klaviyo
FROM orders_complete o
LEFT JOIN ft_purchases_chanel c
ON o.order_created_date = c.order_create_date_chanell