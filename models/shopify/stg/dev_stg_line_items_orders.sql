{# 
stg_line_items_orders es el primer layer de la tabla estandard de ordenes proveniente de Shopify. 
Aquí preparamos los datos para ser consumidos en los distintos layers. 
Se obtiene el detalle nivel producto.
Dentro de una orden pueden haber muchos productos.
Productos < Ordenes 

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



WITH line_item AS (
    SELECT 
         DATE(o.created_at) AS order_created_date,
         DATE(o.closed_at) AS closed_date,
         DATE(o.cancelled_at) AS cancel_order_date, 
        o.id AS order_id,
        o.order_number,
        o.customer:id::STRING AS customer_id,
        f.value:id::INTEGER AS line_item_id,
        f.value:name::STRING AS line_item_name,
        o.financial_status,
        f.value:product_id::INTEGER AS product_id,
        f.value:quantity::INTEGER AS quantity,
        f.value:price::FLOAT AS price,
        f.value:price_set:shop_money:amount::FLOAT AS price_amount,
        f.value:price_set:shop_money:currency_code::STRING AS price_currency,
        f.value:fulfillable_quantity::INTEGER AS fulfillable_quantity,
        f.value:fulfillment_service::STRING AS fulfillment_service,
        f.value:fulfillment_status::STRING AS fulfillment_status,
        f.value:gift_card::BOOLEAN AS is_gift_card,
        f.value:grams::INTEGER AS grams,
        f.value:product_exists::BOOLEAN AS product_exists,
        f.value:properties[0]:name::STRING AS property_name, -- assuming there's only one property
        f.value:properties[0]:value::STRING AS property_value,
        f.value:requires_shipping::BOOLEAN AS requires_shipping,
        f.value:sku::STRING AS sku,
        f.value:taxable::BOOLEAN AS taxable,
        f.value:title::STRING AS title,
        f.value:total_discount::FLOAT AS total_discount,
        f.value:total_discount_set:shop_money:amount::FLOAT AS discount_amount,
        f.value:total_discount_set:shop_money:currency_code::STRING AS discount_currency,
        f.value:variant_id::INTEGER AS variant_id,
        f.value:variant_title::STRING AS variant_title,
        f.value:vendor::STRING AS vendor

    FROM "DEMO_DATABASE"."SCHEMA_DATABASE"."ORDERS" o,
        LATERAL FLATTEN(input => o.line_items) as f
),

first_line_item_purchase AS (

    SELECT
        ft_purchase_line_item_id,
        line_item_name as ft_item_purchase,
        customer_id_ft,
        order_number AS first_order_number,
        order_created_date AS first_purchase_date
    FROM (
        SELECT
            customer_id AS customer_id_ft,
            order_number,
            line_item_id AS ft_purchase_line_item_id,
            order_created_date,
            line_item_name,
            ROW_NUMBER() OVER (PARTITION BY customer_id_ft ORDER BY order_created_date ASC) AS rn
        FROM line_item
        WHERE financial_status = 'paid'
    ) AS ranked_orders
    WHERE rn = 1
)



SELECT 
l.*,
f.first_purchase_date,
f.first_order_number,
f.ft_item_purchase,
CASE WHEN f.ft_purchase_line_item_id = l.line_item_id THEN 1 ELSE 0 END is_ft_item_purchase,
TO_CHAR(DATE_TRUNC('month', l.order_created_date), 'YYYY') as later_year,
TO_CHAR(DATE_TRUNC('month', l.order_created_date), 'MM') as later_month,
TO_CHAR(DATE_TRUNC('month', f.first_purchase_date), 'YYYY') as ft_year,
TO_CHAR(DATE_TRUNC('month', f.first_purchase_date), 'MM') as ft_month,
CASE WHEN later_year - ft_year > 0 then (later_year - ft_year) * 12 ELSE 0 
END diferencia_ano,
later_month - ft_month + diferencia_ano as transcurred_months
FROM line_item l
LEFT JOIN first_line_item_purchase  f
ON l.customer_id = f.customer_id_ft


