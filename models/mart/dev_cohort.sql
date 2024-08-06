{# Cohort es un análisis que nos permite ver el % de recurrencia en cuanto a los meses posteriores de sus primeras compras  #}




WITH line_item AS (

    SELECT * 
    FROM {{ ref('dev_mart_line_items') }}
),

orders AS (

    SELECT * 
    FROM {{ ref('dev_mart_orders') }}


),
{# 
Si se desea poder utilizar una dimension de filtro para cohort , agregar aqui. 
Cuidado con la duplicidad de columnas #}
ft_purchase_item AS (
SELECT
order_number as order_number_ft,
ft_item_purchase
FROM line_item
WHERE is_ft_item_purchase = '1'
),



mix_re AS 
(
SELECT 
f.ft_item_purchase,
o.order_number,
o.transcurred_months AS transcurred_months_re,
o.order_number AS month_total_re,
TO_CHAR(DATE_TRUNC('day', o.order_created_date), 'YYYY-MM') AS re_month_years
FROM orders o
LEFT JOIN ft_purchase_item f
ON o.ft_purchase_order_number = f.order_number_ft
WHERE o.is_ft_purchase = '0'
AND re_month_years = '2024-07'
GROUP BY ALL

),

count_ft_month AS (
SELECT 
line_item_name AS ft_line_item_name,
transcurred_months AS transcurred_months_ft,
COUNT(is_ft_item_purchase) AS month_total_ft,
TO_CHAR(DATE_TRUNC('month', first_purchase_date), 'YYYY-MM') AS ft_month_years
FROM line_item
WHERE is_ft_item_purchase = '1'
GROUP BY ALL
),






























WITH line_item AS (

    SELECT * 
    FROM {{ ref('dev_mart_line_items') }}
),

{# 
Si se desea poder utilizar una dimension de filtro para cohort , agregar aqui. 
Cuidado con la duplicidad de columnas #}


count_ft_month AS (
SELECT 

line_item_name AS ft_line_item_name,
transcurred_months AS transcurred_months_ft,
COUNT(is_ft_item_purchase) AS month_total_ft,
TO_CHAR(DATE_TRUNC('month', first_purchase_date), 'YYYY-MM') AS ft_month_years
FROM line_item
WHERE is_ft_item_purchase = '1'
GROUP BY ALL
),

count_re_month AS (

SELECT 
ft_item_purchase,
line_item_name,
transcurred_months,
COUNT(is_ft_item_purchase) AS  month_total_re,
first_purchase_date,
TO_CHAR(DATE_TRUNC('month', order_created_date), 'YYYY-MM') AS re_month_years
FROM line_item
WHERE is_ft_item_purchase = '0'
AND financial_status = 'paid'
GROUP BY ALL 
)

SELECT 
*
FROM count_re_month r
LEFT JOIN count_ft_month f
ON r.re_month_years = f.ft_month_years
AND r.ft_item_purchase = f.ft_line_item_name









