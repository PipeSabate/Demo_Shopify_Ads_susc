{# Creamos Mart Orders con el fin de poder estructurar datos de distintas fuentes que sean de valor para el anális de las ordenes diarias.
El dato ees bienvenido si:
Puede ser considerado dentro de la estructura de un estado de resultado.
Ayuda al analisis de performance. #}
{{ config(
    materialized='view',
    schema='dev'
) }}

{# 
Tomaremos todos los datos de el primer layer orders  #}
WITH stg_orders AS (
    SELECT *
    FROM {{ ref('dev_stg_orders') }}

),


meta_ads AS (
    SELECT
       *
    FROM {{ ref('dev_stg_fbads_insights_platform_and_device') }}
)


SELECT 
o.*,
m.total_spend_clp,
m.fcb_spend,
m.ig_spend,
m.audience_network_spend,
m.messenger_spend,
m.unknown_spend

FROM stg_orders o
LEFT JOIN meta_ads m
ON o.order_created_date = m.campaign_date
