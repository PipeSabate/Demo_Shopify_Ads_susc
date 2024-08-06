WITH traffic AS(

    SELECT 
    TO_CHAR(TO_DATE(date, 'YYYYMMDD'), 'YYYY-MM-DD') AS traffic_date,
    newusers AS new_users,
    sessions,
    bouncerate AS bounce_rate,
    totalusers AS total_users,
    sessionmedium AS session_medium,
    sessionsource AS session_source,
    screenpageviews AS screen_pages_views
    FROM "DEMO_DATABASE"."GA4"."TRAFFIC_SOURCES"

)


SELECT *
FROM traffic