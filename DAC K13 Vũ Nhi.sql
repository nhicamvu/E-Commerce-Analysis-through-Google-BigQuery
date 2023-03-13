--lưu ý chung: k cần distinct, vì ở dưới mình group by thì nó cũng sẽ gom lại 1 dòng

--Q1
SELECT distinct FORMAT_DATE("%Y%m", PARSE_DATE ("%Y%m%d",date)) AS month, -- k cần distinct, vì ở dưới mình group by thì nó cũng sẽ gom lại 1 dòng
  SUM(totals.visits) AS visits ,
  SUM(totals.pageviews) AS pageviews, 
  SUM(totals.transactions) AS transactions 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
where _table_suffix between '0101' and '0331'
  AND totals.transactions IS NOT NULL 
GROUP BY 1
ORDER BY month;
--correct

--Q2
SELECT 
  distinct trafficSource.source, 
  SUM (totals.visits) AS total_visits,
  SUM (totals.bounces) AS total_no_of_bounces, 
  SUM (totals.bounces/totals.visits) AS bounce_rate  --(sum(totals.Bounces)/sum(totals.visits))* 100 as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` --nếu data lấy tháng 7 thì mình có thể ghi ntn
--where _table_suffix between '0701' and '0731'                    --và bỏ phần này
GROUP BY 1
ORDER BY total_visits DESC;

--Q3
SELECT 'month' AS time_type, 
    FORMAT_DATE("%Y%m", PARSE_DATE ("%Y%m%d",date)) AS time, 
    trafficSource.source,
    sum(product.productRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
UNNEST (hits) AS hits,
UNNEST (hits.product) AS product
WHERE productRevenue IS NOT NULL 
GROUP BY 1,2,3
UNION ALL
SELECT 'week' AS time_type, 
    FORMAT_DATE("%Y%W", PARSE_DATE ("%Y%m%d",date)) AS time, 
    trafficSource.source, 
    sum(product.productRevenue)/1000000 AS revenue 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
UNNEST (hits) AS hits,
UNNEST (hits.product) AS product
WHERE product.productRevenue IS NOT NULL 
GROUP BY 1,2,3;
--correct 


--Q4
WITH purchaser_data as(
      SELECT 
          FORMAT_DATE("%Y%m", PARSE_DATE ("%Y%m%d",date)) AS month, 
          --sum(product.productRevenue)/1000000 AS revenue 
          SUM(totals.pageviews)/COUNT (distinct(fullVisitorID)) AS avg_pageviews_purchase
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST (hits) AS hits,
      UNNEST (hits.product) AS product
      WHERE _table_suffix between '0601' and '0731'
        AND totals.transactions >= 1 
        AND productRevenue IS NOT NULL
      GROUP BY 1
      ),

non_purchaser_data as( 
    SELECT 
        FORMAT_DATE("%Y%m", PARSE_DATE ("%Y%m%d",date)) AS month, 
        --sum(product.productRevenue)/1000000 AS revenue 
        SUM(totals.pageviews)/COUNT (distinct(fullVisitorID)) AS avg_pageviews_non_purchase,
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST (hits) AS hits,
    UNNEST (hits.product) AS product
    WHERE _table_suffix between '0601' and '0731'
      AND totals.transactions IS NULL 
      AND product.productRevenue IS NULL
    GROUP BY 1
    ) 

SELECT 
    month,
    purchaser_data.avg_pageviews_purchase,
    non_purchaser_data.avg_pageviews_non_purchase
FROM purchaser_data
left join non_purchaser_data using(month)
order by month;
--correct

--Q5
SELECT 
    FORMAT_DATE("%Y%m", PARSE_DATE ("%Y%m%d",date)) AS month,  
    SUM(totals.transactions)/COUNT (distinct(fullVisitorID)) AS Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
UNNEST (hits) AS hits,
UNNEST (hits.product) AS product
WHERE _table_suffix between '0701' and '0731'
  AND totals.transactions >= 1 
  AND totals.totalTransactionRevenue IS NOT NULL
  AND product.productRevenue IS NOT NULL
GROUP BY 1;
--correct

--Q6
SELECT 
    FORMAT_DATE("%Y%m", PARSE_DATE ("%Y%m%d",date)) AS month,  
    AVG ((product.productRevenue)/(totals.visits)) AS Avg_revenue_by_user_per_visit
    --((sum(product.productRevenue)/sum(totals.visits))/power(10,6)) as avg_revenue_by_user_per_visit      --ghi như vậy sẽ hợp lý về mặt logic hơn
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
UNNEST (hits) AS hits,
UNNEST (hits.product) AS product
WHERE _table_suffix between '0701' and '0731'
  AND totals.transactions IS NOT NULL
  AND product.productRevenue IS NOT NULL
GROUP BY 1;

--Q7
SELECT 
    product.v2productname AS other_purchased_product,	
    SUM(product.productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
UNNEST (hits) AS hits,
UNNEST (hits.product) AS product
WHERE fullvisitorid in
                  (SELECT distinct fullvisitorid
                  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
                  UNNEST (hits) AS hits,
                  UNNEST (hits.product) AS product
                  WHERE product.v2productname = "Youtube Men's Vintage Henley"
                  AND product.productRevenue IS NOT NULL)
  AND product.v2productname != "Youtube Men's Vintage Henley"
  AND product.productRevenue IS NOT NULL 
GROUP BY other_purchased_product
ORDER BY quantity DESC;


--bài làm khá tốt, 1 số đoạn a chỉnh thụt dòng cho đẹp thoi