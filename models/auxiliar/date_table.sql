{# Creamos una tabla de Jerarquias de fecha. 

Esto con el objetivo de poder segmentar en el analisis en distintas lineas de tiempo  #}
{{ config(
    materialized='ephemeral',
    schema='dev'
) }}


WITH RECURSIVE date_series AS (
    SELECT
        TO_DATE('2023-01-02') AS date  -- Aseguramos que la serie comienza un lunes
    UNION ALL
    SELECT
        DATEADD(day, 1, date) AS date  -- Usamos DATEADD para sumar días
    FROM date_series
    WHERE date < TO_DATE('2025-12-31')
),

week_series AS (
    SELECT
        date,
        YEAR(date) AS year,
        MONTH(date) AS month,
        DAY(date) AS day,
        WEEKOFYEAR(date) AS week_of_year,  -- Número de la semana en el año
        -- Ajustar al lunes más cercano si el día es domingo (0), restamos 6 días, de lo contrario, restamos DAYOFWEEK - 1
        DATEADD(day, -1 * (CASE WHEN DAYOFWEEK(date) = 0 THEN 6 ELSE DAYOFWEEK(date) - 1 END), date) AS week_start,
        -- Ajustar al domingo más cercano sumando los días que faltan para el domingo
        DATEADD(day, (CASE WHEN DAYOFWEEK(date) = 0 THEN 0 ELSE 7 - DAYOFWEEK(date) END), date) AS week_end
    FROM date_series
)

SELECT
    date AS date_raw,
    year,
    month,
    CASE 
        WHEN month = 1 THEN 'Enero'
        WHEN month = 2 THEN 'Febrero'
        WHEN month = 3 THEN 'Marzo'
        WHEN month = 4 THEN 'Abril'
        WHEN month = 5 THEN 'Mayo'
        WHEN month = 6 THEN 'Junio'
        WHEN month = 7 THEN 'Julio'
        WHEN month = 8 THEN 'Agosto'
        WHEN month = 9 THEN 'Septiembre'
        WHEN month = 10 THEN 'Octubre'
        WHEN month = 11 THEN 'Noviembre'
        WHEN month = 12 THEN 'Diciembre'
    END AS month_name,
    TO_CHAR(year) ||'/' || TO_CHAR(month) AS year_month,
    day,
    week_of_year ,  -- Número de la semana en el año
    -- Comenzamos la semana en lunes, entonces ajustamos si week_start no es lunes
    --CASE WHEN DAYOFWEEK(week_start) != 1 THEN DATEADD(day, -1 * (DAYOFWEEK(week_start) - 1), week_start) ELSE week_start END AS week_start_corrected,
    -- Terminamos la semana en domingo, entonces ajustamos si week_end no es domingo
   -- CASE WHEN DAYOFWEEK(week_end) != 0 THEN DATEADD(day, 7 - DAYOFWEEK(week_end), week_end) ELSE week_end END, AS week_end_corrected,
    DAYOFWEEK(date) AS weekday,
    DENSE_RANK() OVER (PARTITION BY year, month ORDER BY week_start) AS week_of_month,
   -- TO_CHAR(week_start, 'YYYY-MM-DD') || ' / ' || TO_CHAR(week_end, 'YYYY-MM-DD') AS week_range_uncorrected,  -- Rango sin corrección
    TO_CHAR(CASE WHEN DAYOFWEEK(week_start) != 1 THEN DATEADD(day, -1 * (DAYOFWEEK(week_start) - 1), week_start) ELSE week_start END, 'YYYY-MM-DD') 
    || ' / ' || 
    TO_CHAR(CASE WHEN DAYOFWEEK(week_end) != 0 THEN DATEADD(day, 7 - DAYOFWEEK(week_end), week_end) ELSE week_end END, 'YYYY-MM-DD') AS week_range, -- Rango con corrección
    CASE
        WHEN month BETWEEN 1 AND 6 THEN 'S1'
        ELSE 'S2'
    END AS semester,
    CASE
        WHEN month BETWEEN 1 AND 3 THEN 'Q1'
        WHEN month BETWEEN 4 AND 6 THEN 'Q2'
        WHEN month BETWEEN 7 AND 9 THEN 'Q3'
        ELSE 'Q4'
    END AS quarter
FROM week_series
