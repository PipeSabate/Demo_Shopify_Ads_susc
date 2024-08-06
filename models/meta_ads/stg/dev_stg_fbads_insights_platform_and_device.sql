{# dev_stg_fbads_insights_platform_and_device es el primer layer de la tabla estandard de inversión proveniente de Meta Ads. 
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


{# De momento solamente extraeremos la información de Inversión de Meta ads. 
Queremos profundizar el performance mediante GA4  #}

SELECT
    date_trunc('day', date_start) AS campaign_date,
    SUM(spend) AS total_spend_clp,
    SUM(CASE WHEN publisher_platform = 'facebook' THEN spend ELSE 0 END) AS fcb_spend,
    SUM(CASE WHEN publisher_platform = 'instagram' THEN spend ELSE 0 END) AS ig_spend,
    SUM(CASE WHEN publisher_platform = 'audience_network' THEN spend ELSE 0 END) AS audience_network_spend,
    SUM(CASE WHEN publisher_platform = 'messenger' THEN spend ELSE 0 END) AS messenger_spend,
    SUM(CASE WHEN publisher_platform = 'unknown' THEN spend ELSE 0 END) AS unknown_spend
FROM "DEMO_DATABASE"."SCHEMA_DATABASE"."ADS_INSIGHTS_PLATFORM_AND_DEVICE"
GROUP BY campaign_date
