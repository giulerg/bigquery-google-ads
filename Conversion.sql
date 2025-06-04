/*
  Task 1.
  Main events for 2021 from GA4 
  Includes information about users, devices, traffic sources, sessions and etc
*/

select 
  FORMAT_TIMESTAMP('%d-%m-%Y %H:%M', TIMESTAMP_MICROS(event_timestamp)) as date_time,
  user_pseudo_id as user_id, 
  (
    select value.int_value 
    from UNNEST(event_params)
    where key = 'ga_session_id' 
  ) as session_id,
  event_name,
  geo.country,
  device.category, 
  traffic_source.source,
  traffic_source.medium,
  traffic_source.name as campaign
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
where 
  _TABLE_SUFFIX LIKE '2021%' AND
  event_name in (
    'session_start', 'view_item', 
    'add_to_cart', 'begin_checkout', 
    'add_shipping_info', 'add_payment_info',
    'purchase'
  );

/*
  Task 2.
  This query shows daily numbers for steps in 
  buying = adding to cart, starting checkout and buying 
  grouped by date and traffic

*/
 --CTE user_history: extract all user events 
 with user_history as (
  select 
    FORMAT_TIMESTAMP('%d-%m-%Y', TIMESTAMP_MICROS(event_timestamp)) as event_date ,
    user_pseudo_id as user_id, 
    (
      select value.int_value 
      from UNNEST(event_params)
      where key = 'ga_session_id' 
    ) as session_id,
    event_name, 
    traffic_source.source,
    traffic_source.medium,
    traffic_source.name as campaign
  from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
 ),

 --CTE user_session: mergin user_id and session_id 
 --to create uniquer id for each user session
 user_session as (
  select distinct
    concat(user_id, '_', session_id) as unique_user,
    event_date,
    event_name,
    source,
    medium,
    campaign,
  from user_history  
 )

--counts user session and calcukates conversion
select 
  event_date,
  source,
  medium,
  campaign,
  count(distinct unique_user) as user_sessions_count,
  round(sum(case when event_name = 'add_to_cart'  then 1 else 0 end) /  count(distinct unique_user) * 100, 2) as visit_to_cart,
  round(sum(case when event_name = 'begin_checkout'  then 1 else 0 end) / count(distinct unique_user) * 100, 2) as visit_to_checkout,
  round(sum(case when event_name = 'purchase'  then 1 else 0 end) /  count(distinct unique_user) * 100, 2) as visit_to_purchase 
from user_session
group by event_date,
  source,
  medium,
  campaign 
order by visit_to_purchase desc
--We see that most purchase events happened in November 2020.