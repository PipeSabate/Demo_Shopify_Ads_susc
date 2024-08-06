WITH overview as (

SELECT 
TO_CHAR(TO_DATE(date, 'YYYYMMDD'), 'YYYY-MM-DD') AS overview_date,
newusers AS new_users,
sessions,
bouncerate,
totalusers as total_users
FROM "DEMO_DATABASE"."GA4"."WEBSITE_OVERVIEW"

)

SELECT * 
FROM overview