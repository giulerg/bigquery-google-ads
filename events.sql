--Task 3
/*
   This query shows the number of sessions, 
   purchases and the conversion rate
   for each page path
*/

--this cte extracts purchase and session_start events and clean page pathes: 
WITH
  page_events AS (
  SELECT
    user_pseudo_id AS user_id,
    (
    SELECT
      value.int_value
    FROM
      UNNEST(event_params)
    WHERE
      KEY = 'ga_session_id' ) AS session_id,
    REGEXP_EXTRACT((
      SELECT
        value.string_value
      FROM
        UNNEST(event_params)
      WHERE
        KEY = 'page_location' ), r'^https?://[^/]+(/[^?#]*)') AS page_path,
    event_name
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
    _TABLE_SUFFIX LIKE '2020%'
    AND event_name IN ('session_start','purchase')),

--this cte merges userID and sessionID to create a unique session identifier
sessions AS (
  SELECT
    DISTINCT CONCAT(user_id, '_', session_id) AS unique_user,
    page_path,
    event_name
  FROM
    page_events ),

--this cte selects rows where user had a purchase
purchase_cte AS (
  SELECT
    unique_user,
    1 AS is_purchased
  FROM
    sessions
  WHERE
    event_name = 'purchase' ),

--this cte joins two tables  
joined_table AS (
  SELECT
    p.*,
    c.is_purchased
  FROM
    sessions AS p
  LEFT JOIN
    purchase_cte c
  ON
    p.unique_user = c.unique_user
  WHERE
    p.event_name != 'purchase' )

--results
SELECT
  page_path,
  COUNT(DISTINCT unique_user) AS session_count,
  sum (CASE
      WHEN is_purchased = 1 THEN 1
      ELSE 0
  END
    ) AS purchase_count,
  ROUND(SUM(CASE
        WHEN is_purchased = 1 THEN 1
        ELSE 0
    END
      ) / COUNT( unique_user) * 100, 2) AS conversion_rate
FROM
  joined_table
GROUP BY
  page_path;


--Task 4
/*
  This query calculates  4 variables: 
  1. session_engaged - shows if the  user was active during the session
  2. engagement_time_msec -  the total engagement time for session 
  3. is_purchased - shows wheither user  made a purchase in this session
  4. 2 correlations  
      the first is between  session_engaged and is_purchased
      the second is between engagement_time and is_purchased
*/

--This cte extracts the main data for the future calculations
WITH session AS (
  SELECT 
    user_pseudo_id AS user_id,
      (
        SELECT
          value.int_value
        FROM
          UNNEST(event_params)
        WHERE
          KEY = 'ga_session_id' ) AS session_id,
      IFNULL((
        SELECT
          value.int_value
        FROM
          UNNEST(event_params)
        WHERE
          KEY = 'session_engaged')
        , 0 ) AS session_engaged,
      IFNULL((
        SELECT
          value.int_value
        FROM
          UNNEST(event_params)
        WHERE
          KEY = 'engagement_time_msec')
        , 0 ) AS engagement_time_msec,
      (CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) is_purchased
        
  FROM   
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
    _TABLE_SUFFIX LIKE '2020%'
),

--this cte shows answers to questions 1-3
user_information AS (
  SELECT 
    concat(user_id, '_', session_id) as unique_user, 
    MAX(session_engaged) as session_engaged,
    SUM(engagement_time_msec) as  engagement_time,
    MAX(is_purchased) as is_purchased 
  FROM session
  GROUP BY unique_user
)

--4 correlation 
SELECT 
  CORR(session_engaged, is_purchased) AS corr_engaged_time,
  CORR(engagement_time, is_purchased) as corr_time_purchased
FROM user_information

---------------------------------------------------------------------------------------------------------------------------------------------------------------------
/* Results

  We analyzed variables that are important for business to understand what may lead  users to make a purchase.
  To answer this question we used data from Google Analytics.
  First  we analyzed  the conversation rate for each page path.
  I focused on the most viewed pages (with 1000 and more viewers) :
      The pages basket, Shop by Brand/Google and /Google Redesign/Apparel/Mens have the highest conversation rate (more or equal 3%). This suggests that users who srart their sessions from these pages are more likely to make a purchase.
    Users who start from pages / , asearch.html and  signin.html  have thr lowest conversation rate (less or equal 1.89)
  Secondly I analysed the correlation between main parameters: purchase events, session_engaged and engagement time . At the end I found a positive correlation (0.326) only between engagement time and purcchased value . It means that users who spend more time engaged are more likely to purchase.
  
*/
