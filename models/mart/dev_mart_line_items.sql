{# Creamos Mart Line Items con el fin de poder estructurar datos de distintas fuentes que sean de valor para el anális de las ordenes diarias.
El dato ees bienvenido si:
Puede ser considerado dentro de la estructura de un estado de resultado.
Ayuda al analisis de performance. #}


{{ config(
    materialized='ephemeral',
    schema='dev'
) }}


WITH line_item AS (

    SELECT * 
    FROM {{ ref('dev_stg_line_items_orders') }}
)

SELECT * 
FROM line_item