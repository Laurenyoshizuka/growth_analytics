-- orders per month per store 
orders per month per store
SELECT 
    DATA:shopifyShopURL::STRING AS store,
   -- TO_CHAR(DATA:shopifyOrderProcessedAt::TIMESTAMP_NTZ, 'YYYY-MM-DD') AS shopifyOrderProcessedAt,
    TO_CHAR(DATE_TRUNC('month', DATA:shopifyOrderProcessedAt::TIMESTAMP_NTZ), 'YYYY-MM-DD') AS month,
    COUNT(DISTINCT DATA:shopifyOrderId::STRING) AS order_count
FROM ANALYTICS_ENG_INTERVIEW.DATA.PIXEL
WHERE DATA:shopifyPageType::STRING = 'thank_you'
GROUP BY 1,2
ORDER BY 1,2
;

-- attribution model
WITH order_events AS (
  -- Extract all order events
  SELECT 
    DATA:timestamp::INTEGER AS timestamp,
    DATA:userId AS userId,
    DATA:sessionId AS sessionId,
    DATA:shopifyShopURL AS shopifyShopURL,
    DATA:ip AS ip_address,
    DATA:shopifyOrderId AS shopifyOrderId,
    DATA:shopifyOrderProcessedAt AS shopifyOrderProcessedAt,
    DATA:shopifyOrderTotalPrice AS shopifyOrderTotalPrice
  FROM 
    ANALYTICS_ENG_INTERVIEW.DATA.PIXEL
  WHERE 
    DATA:shopifyOrderId IS NOT NULL
)

,touchpoint_events AS (
  -- Extract all touchpoint events with attribution source
  SELECT 
    DATA:timestamp::INTEGER AS timestamp,
    DATA:userId AS userId,
    DATA:sessionId AS sessionId,
    DATA:shopifyShopURL AS shopifyShopURL,
    DATA:ip AS ip_address,
    COALESCE(DATA:utmSource, 
      CASE 
        WHEN DATA:pageReferrer IS NULL OR DATA:pageReferrer = '' THEN 'direct'
        WHEN DATA:pageReferrer LIKE '%google%' THEN 'google'
        WHEN DATA:pageReferrer LIKE '%facebook%' THEN 'facebook'
        WHEN DATA:pageReferrer LIKE '%instagram%' THEN 'instagram'
        ELSE 'referral'
      END) AS attribution_source,
    DATA:pageReferrer AS pageReferrer,
    DATA:utmMedium AS utmMedium,
    DATA:utmCampaign AS utmCampaign
  FROM 
    ANALYTICS_ENG_INTERVIEW.DATA.PIXEL
)

,customer_journey AS (
  -- Connect orders to touchpoints using IP address and userId
  SELECT
    o.shopifyOrderId,
    o.shopifyShopURL,
    o.shopifyOrderProcessedAt,
    o.shopifyOrderTotalPrice,
    t.timestamp AS touchpoint_timestamp,
    t.attribution_source,
    t.pageReferrer,
    t.utmMedium,
    t.utmCampaign,
    CASE 
      WHEN o.userId = t.userId THEN 'userId_match'
      WHEN (o.ip_address = t.ip_address AND o.timestamp = t.timestamp) THEN 'ip_match'
      ELSE 'no_match'
    END AS match_type,
    -- Lookback window X days before purchase
    CASE 
      WHEN t.timestamp <= o.timestamp AND 
           t.timestamp >= (o.timestamp - 90*24*60*60) -- X days in seconds
      THEN 1 ELSE 0 
    END AS is_within_lookback
  FROM 
    order_events o
  JOIN 
    touchpoint_events t
  ON 
    (o.ip_address = t.ip_address AND o.timestamp = t.timestamp) 
    AND o.shopifyShopURL = t.shopifyShopURL
  WHERE
    t.timestamp <= o.timestamp -- Only include touchpoints before purchase
),

last_click_attribution AS (
  -- Get the last touchpoint before each purchase within lookback window
  SELECT 
    shopifyOrderId,
    shopifyShopURL,
    shopifyOrderProcessedAt,
    shopifyOrderTotalPrice,
    attribution_source,
    pageReferrer,
    utmMedium,
    utmCampaign,
    match_type
  FROM (
    SELECT 
      *,
      ROW_NUMBER() OVER (
        PARTITION BY shopifyOrderId 
        ORDER BY touchpoint_timestamp DESC
      ) AS touchpoint_order
    FROM 
      customer_journey
    WHERE 
      is_within_lookback = 1
  ) ranked_touchpoints
  WHERE 
    touchpoint_order = 1
)

-- Final attribution report by source, medium, campaign
SELECT 
  --t.tenant_id AS brand,
  lca.shopifyShopURL as store,
  DATE_TRUNC('month', TO_DATE(lca.shopifyOrderProcessedAt)) AS month,
  lca.attribution_source,
  lca.pageReferrer,
  COALESCE(lca.utmMedium, 'none') AS medium,
  COALESCE(lca.utmCampaign, 'none') AS campaign,
  COUNT(DISTINCT lca.shopifyOrderId) AS attributed_orders,
  SUM(lca.shopifyOrderTotalPrice) AS attributed_revenue
FROM 
  last_click_attribution lca
-- JOIN 
--   ANALYTICS_ENG_INTERVIEW.DATA.TENANTS t  -- Join the TENANTS table to get the brand (tenant)
--   ON lca.shopifyShopURL = t.shopify_url  -- Match shopifyShopURL with the shopify_url in the TENANTS table
GROUP BY 
    1,2,3,4,5,6
ORDER BY 
    lca.shopifyShopURL, month, attributed_orders DESC;

-- touchpoint count along the customer journey by attribution source
WITH order_events AS (
  SELECT 
    DATA:timestamp::INTEGER AS timestamp,
    DATA:userId AS userId,
    DATA:sessionId AS sessionId,
    DATA:shopifyShopURL AS shopifyShopURL,
    DATA:ip AS ip_address,
    DATA:shopifyOrderId AS shopifyOrderId,
    DATA:shopifyOrderProcessedAt AS shopifyOrderProcessedAt,
    DATA:shopifyOrderTotalPrice AS shopifyOrderTotalPrice
  FROM 
    ANALYTICS_ENG_INTERVIEW.DATA.PIXEL
  WHERE 
    DATA:shopifyOrderId IS NOT NULL
)
,touchpoint_events AS (
  SELECT 
    DATA:timestamp::INTEGER AS timestamp,
    DATA:userId AS userId,
    DATA:sessionId AS sessionId,
    DATA:shopifyShopURL AS shopifyShopURL,
    DATA:ip AS ip_address,
    COALESCE(DATA:utmSource, 
      CASE 
        WHEN DATA:pageReferrer IS NULL OR DATA:pageReferrer = '' THEN 'direct'
        WHEN DATA:pageReferrer LIKE '%google%' THEN 'google'
        WHEN DATA:pageReferrer LIKE '%facebook%' THEN 'facebook'
        WHEN DATA:pageReferrer LIKE '%instagram%' THEN 'instagram'
        ELSE 'referral'
      END) AS attribution_source
  FROM 
    ANALYTICS_ENG_INTERVIEW.DATA.PIXEL
)
,customer_journey AS (
  SELECT
    o.shopifyOrderId,
    o.shopifyShopURL,
    o.shopifyOrderProcessedAt,
    o.shopifyOrderTotalPrice,
    t.timestamp AS touchpoint_timestamp,
    t.attribution_source,
    ROW_NUMBER() OVER (PARTITION BY o.shopifyOrderId ORDER BY t.timestamp) AS touchpoint_step
  FROM 
    order_events o
  JOIN 
    touchpoint_events t
  ON 
    o.ip_address = t.ip_address AND o.timestamp = t.timestamp
  WHERE
    t.timestamp <= o.timestamp -- Only include touchpoints before purchase
)
SELECT 
    shopifyOrderId,
    shopifyShopURL,
    touchpoint_step,
    attribution_source
FROM customer_journey
ORDER BY shopifyOrderId, touchpoint_step;