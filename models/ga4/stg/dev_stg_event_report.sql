WITH events AS (

SELECT 

TO_CHAR(TO_DATE(date, 'YYYYMMDD'), 'YYYY-MM-DD') AS event_date,
eventname as event_name,
eventcount as event_count,
totalusers as total_users,
totalrevenue as total_revenue

FROM "DEMO_DATABASE"."GA4"."EVENTS_REPORT"

)

SELECT 
DATE_TRUNC('month',DATE(event_date)) as month,
event_name,
SUM(event_count),
FROM events
