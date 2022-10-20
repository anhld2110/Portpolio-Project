
-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  sum(totals.visits) as visits,
  sum(totals.pageviews) as pageviews,
  sum(totals.transactions) as transactions,
  sum(totals.totalTransactionRevenue)/1000000 as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
Where _table_suffix between '20170101' and '20170331'
group by 1  

-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
SELECT
  trafficSource.source as source,
  sum(totals.visits) as total_visits,
  sum(totals.bounces) as total_no_of_bounces,
  round(sum(totals.bounces)/sum(totals.visits)*100,2) as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
Where _table_suffix between '20170701' and '20170731'
group by 1
order by 2 desc

-- Query 3: Revenue by traffic source by week, by month in June 2017
#standardSQL
SELECT
  'Month' as time_type,
  format_date("%Y%m", parse_date("%Y%m%d", date)) as time,
  trafficSource.source as source,
  sum(totals.totalTransactionRevenue)/1000000 as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
Where (_table_suffix between '20170601' and '20170630')
and totals.totalTransactionRevenue is not null
group by time,source  
union all 
select
  'Week' as time_type,
  format_date("%Y%V", parse_date("%Y%m%d", date)) as time,
  trafficSource.source as source,
  sum(totals.totalTransactionRevenue)/1000000 as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
Where (_table_suffix between '20170601' and '20170630') 
and totals.totalTransactionRevenue is not null
group by time,source    
order by revenue desc

--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
with cte1 as
  (SELECT
    format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
    sum(totals.pageviews) / count(distinct fullvisitorid) as avg_pageviews_purchase
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
  Where (_table_suffix between '20170601' and '20170731')
  and totals.transactions >=1
  group by format_date("%Y%m", parse_date("%Y%m%d", date))),

cte2 as
  (SELECT
    format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
    sum(totals.pageviews) / count(distinct fullvisitorid) as avg_pageviews_non_purchase
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
  Where (_table_suffix between '20170601' and '20170731')
  and totals.transactions is null
  group by format_date("%Y%m", parse_date("%Y%m%d", date))) 

select 
  cte1.month,
  avg_pageviews_purchase,
  avg_pageviews_non_purchase
from cte1
join cte2
using(month)
order by month 


-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  sum(totals.transactions) / count(distinct fullvisitorid) as Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
Where (_table_suffix between '20170701' and '20170731')
and totals.transactions >=1
group by 1

-- Query 06: Average amount of money spent per session
#standardSQL
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  sum(totals.totalTransactionRevenue) / count(visitid) as avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
Where (_table_suffix between '20170701' and '20170731')
and totals.transactions IS NOT NULL
group by 1



-- Query 07: Products purchased by customers who purchased product A (Classic Ecommerce)
#standardSQL
SELECT 
  v2productname as other_purchased_products,
  sum(productQuantity) as quantity
FROM 
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`, 
  unnest(hits) as hits, 
  unnest(hits.product) as product
Where _table_suffix between '20170701' and '20170731' 

and fullvisitorid in --select customers who purchased YouTube Men's Vintage Henley
  (SELECT distinct
    fullvisitorid   
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`, unnest(hits) as hits, unnest(hits.product) as product
  Where _table_suffix between '20170701' and '20170731' 
  and productrevenue is not null 
  and product.v2ProductName="YouTube Men's Vintage Henley")

and productrevenue is not null 
and v2productname <> "YouTube Men's Vintage Henley"
Group by v2productname
order by quantity desc


--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
with cte as
  (SELECT 
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    count(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,
    count(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,
    count(CASE WHEN eCommerceAction.action_type = '6' THEN product.v2ProductName END) as num_purchase
  unnest(hits) as hits
  where _table_suffix between '20170101' and '20170331' 
  group by 1

select 
  month,
  num_product_view,
  num_addtocart,
  num_purchase,
  round(num_addtocart/num_product_view * 100,2) as add_to_cart_rate,
  round(num_purchase/num_product_view * 100,2) as purchase_rate
from cte
order by month 





