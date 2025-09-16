-- ===================================================================
-- ADVANCED TESTING QUERIES FOR ATABOT
-- Complex business intelligence queries to test AI capabilities
-- ===================================================================

-- Set schema
SET search_path TO atabot_testing;

-- ===================================================================
-- 1. SALES TREND ANALYSIS QUERIES
-- ===================================================================

-- Q1: Monthly sales growth analysis with YoY comparison
-- Test: "Bagaimana trend penjualan bulanan tahun ini dibanding tahun lalu?"
SELECT
    year,
    month,
    total_revenue,
    total_orders,
    LAG(total_revenue) OVER (ORDER BY year, month) AS prev_month_revenue,
    ROUND(
        (total_revenue - LAG(total_revenue) OVER (ORDER BY year, month)) /
        NULLIF(LAG(total_revenue) OVER (ORDER BY year, month), 0) * 100, 2
    ) AS month_over_month_growth_pct,
    SUM(total_revenue) OVER (ORDER BY year, month) AS cumulative_revenue
FROM monthly_metrics
ORDER BY year, month;

-- Q2: Seasonal sales patterns by category
-- Test: "Kategori produk mana yang paling laku di musim tertentu?"
SELECT
    pc.category_name,
    EXTRACT(MONTH FROM so.order_date) as month,
    CASE
        WHEN EXTRACT(MONTH FROM so.order_date) IN (12, 1, 2) THEN 'Q4-Q1 (Holiday/New Year)'
        WHEN EXTRACT(MONTH FROM so.order_date) IN (3, 4, 5) THEN 'Q1-Q2 (Spring/Ramadan)'
        WHEN EXTRACT(MONTH FROM so.order_date) IN (6, 7, 8) THEN 'Q2-Q3 (Summer/Back to School)'
        WHEN EXTRACT(MONTH FROM so.order_date) IN (9, 10, 11) THEN 'Q3-Q4 (Fall/Pre-Holiday)'
    END AS season,
    COUNT(DISTINCT so.order_id) as total_orders,
    SUM(soi.quantity) as total_quantity,
    SUM(soi.line_total) as total_revenue,
    ROUND(AVG(soi.unit_price), 0) as avg_price
FROM sales_orders so
JOIN sales_order_items soi ON so.order_id = soi.order_id
JOIN products p ON soi.product_id = p.product_id
JOIN product_categories pc ON p.category_id = pc.category_id
WHERE so.order_status NOT IN ('Cancelled', 'Returned')
GROUP BY pc.category_name, EXTRACT(MONTH FROM so.order_date), season
ORDER BY pc.category_name, month;

-- ===================================================================
-- 2. CUSTOMER SEGMENTATION & BEHAVIOR ANALYSIS
-- ===================================================================

-- Q3: Customer lifetime value segmentation with behavior patterns
-- Test: "Segmentasi customer berdasarkan nilai lifetime dan pola pembelian"
WITH customer_metrics AS (
    SELECT
        c.customer_id,
        c.customer_code,
        c.first_name || ' ' || c.last_name AS full_name,
        c.registration_date,
        c.city,
        c.acquisition_channel,
        COUNT(DISTINCT so.order_id) as total_orders,
        SUM(so.total_amount) as lifetime_value,
        AVG(so.total_amount) as avg_order_value,
        MAX(so.order_date) as last_order_date,
        MIN(so.order_date) as first_order_date,
        EXTRACT(DAYS FROM MAX(so.order_date) - MIN(so.order_date)) as customer_lifespan_days,
        COUNT(DISTINCT EXTRACT(MONTH FROM so.order_date)) as active_months,
        STRING_AGG(DISTINCT sc.channel_name, ', ') as used_channels
    FROM customers c
    LEFT JOIN sales_orders so ON c.customer_id = so.customer_id
        AND so.order_status NOT IN ('Cancelled', 'Returned')
    LEFT JOIN sales_channels sc ON so.channel_id = sc.channel_id
    WHERE c.is_active = true
    GROUP BY c.customer_id, c.customer_code, c.first_name, c.last_name,
             c.registration_date, c.city, c.acquisition_channel
)
SELECT
    *,
    CASE
        WHEN lifetime_value >= 50000000 AND total_orders >= 10 THEN 'VIP Champions'
        WHEN lifetime_value >= 20000000 AND total_orders >= 5 THEN 'Loyal Customers'
        WHEN lifetime_value >= 5000000 AND total_orders >= 3 THEN 'Growing Customers'
        WHEN total_orders >= 2 THEN 'Repeat Buyers'
        ELSE 'One-Time Buyers'
    END as customer_segment_behavior,
    CASE
        WHEN EXTRACT(DAYS FROM NOW() - last_order_date) <= 30 THEN 'Highly Active'
        WHEN EXTRACT(DAYS FROM NOW() - last_order_date) <= 90 THEN 'Active'
        WHEN EXTRACT(DAYS FROM NOW() - last_order_date) <= 180 THEN 'At Risk'
        ELSE 'Churned'
    END as engagement_status,
    ROUND(lifetime_value / NULLIF(customer_lifespan_days, 0), 0) as daily_value_rate
FROM customer_metrics
ORDER BY lifetime_value DESC;

-- Q4: Customer acquisition channel effectiveness
-- Test: "Channel marketing mana yang paling efektif untuk akuisisi customer?"
SELECT
    c.acquisition_channel,
    COUNT(DISTINCT c.customer_id) as customers_acquired,
    SUM(c.total_lifetime_value) as total_revenue_generated,
    AVG(c.total_lifetime_value) as avg_customer_ltv,
    AVG(c.total_orders) as avg_orders_per_customer,
    COUNT(DISTINCT CASE WHEN c.total_orders >= 3 THEN c.customer_id END) as repeat_customers,
    ROUND(
        COUNT(DISTINCT CASE WHEN c.total_orders >= 3 THEN c.customer_id END)::NUMERIC /
        NULLIF(COUNT(DISTINCT c.customer_id), 0) * 100, 2
    ) as repeat_rate_pct,
    -- Calculate ROI estimate (assuming acquisition costs)
    CASE c.acquisition_channel
        WHEN 'Google Ads' THEN SUM(c.total_lifetime_value) - (COUNT(DISTINCT c.customer_id) * 250000)
        WHEN 'Facebook Ads' THEN SUM(c.total_lifetime_value) - (COUNT(DISTINCT c.customer_id) * 180000)
        WHEN 'Instagram' THEN SUM(c.total_lifetime_value) - (COUNT(DISTINCT c.customer_id) * 150000)
        WHEN 'TikTok' THEN SUM(c.total_lifetime_value) - (COUNT(DISTINCT c.customer_id) * 120000)
        WHEN 'Referral' THEN SUM(c.total_lifetime_value) - (COUNT(DISTINCT c.customer_id) * 100000)
        WHEN 'Social Media' THEN SUM(c.total_lifetime_value) - (COUNT(DISTINCT c.customer_id) * 80000)
        ELSE SUM(c.total_lifetime_value)
    END as estimated_roi
FROM customers c
GROUP BY c.acquisition_channel
ORDER BY total_revenue_generated DESC;

-- ===================================================================
-- 3. PRODUCT PERFORMANCE & INVENTORY OPTIMIZATION
-- ===================================================================

-- Q5: Product performance matrix with inventory turnover
-- Test: "Produk mana yang perlu direstock dan mana yang slow moving?"
WITH product_performance AS (
    SELECT
        p.product_id,
        p.product_code,
        p.product_name,
        pc.category_name,
        b.brand_name,
        pi.current_stock,
        pi.minimum_stock,
        pi.average_monthly_usage,
        COALESCE(SUM(soi.quantity), 0) as total_sold_ytd,
        COALESCE(SUM(soi.line_total), 0) as total_revenue_ytd,
        COALESCE(SUM(soi.line_profit), 0) as total_profit_ytd,
        COUNT(DISTINCT so.order_id) as total_orders,
        AVG(pr.rating) as avg_rating,
        COUNT(pr.review_id) as review_count
    FROM products p
    LEFT JOIN product_categories pc ON p.category_id = pc.category_id
    LEFT JOIN brands b ON p.brand_id = b.brand_id
    LEFT JOIN product_inventory pi ON p.product_id = pi.product_id AND pi.warehouse_id = 1
    LEFT JOIN sales_order_items soi ON p.product_id = soi.product_id
    LEFT JOIN sales_orders so ON soi.order_id = so.order_id
        AND so.order_status NOT IN ('Cancelled', 'Returned')
        AND so.order_date >= '2024-01-01'
    LEFT JOIN product_reviews pr ON p.product_id = pr.product_id
    WHERE p.is_active = true
    GROUP BY p.product_id, p.product_code, p.product_name, pc.category_name,
             b.brand_name, pi.current_stock, pi.minimum_stock, pi.average_monthly_usage
)
SELECT
    *,
    CASE
        WHEN current_stock <= minimum_stock THEN 'CRITICAL - Restock Now'
        WHEN current_stock <= (minimum_stock * 1.5) THEN 'LOW - Restock Soon'
        WHEN current_stock >= (minimum_stock * 5) AND average_monthly_usage < 10 THEN 'EXCESS - Slow Moving'
        ELSE 'NORMAL'
    END as stock_status,
    CASE
        WHEN average_monthly_usage > 0 THEN ROUND(current_stock / average_monthly_usage, 1)
        ELSE 999
    END as months_of_inventory,
    ROUND(total_sold_ytd / NULLIF(average_monthly_usage * 12, 0), 2) as inventory_turnover_ratio,
    ROUND(total_profit_ytd / NULLIF(total_revenue_ytd, 0) * 100, 2) as profit_margin_pct
FROM product_performance
ORDER BY total_revenue_ytd DESC;

-- Q6: Cross-selling opportunities analysis
-- Test: "Produk apa yang sering dibeli bersamaan untuk cross-selling?"
WITH order_combinations AS (
    SELECT
        soi1.product_id as product_a,
        soi2.product_id as product_b,
        COUNT(*) as times_bought_together,
        COUNT(DISTINCT soi1.order_id) as orders_with_both
    FROM sales_order_items soi1
    JOIN sales_order_items soi2 ON soi1.order_id = soi2.order_id
        AND soi1.product_id < soi2.product_id  -- Avoid duplicates
    JOIN sales_orders so ON soi1.order_id = so.order_id
    WHERE so.order_status NOT IN ('Cancelled', 'Returned')
    GROUP BY soi1.product_id, soi2.product_id
    HAVING COUNT(*) >= 2  -- At least bought together 2 times
)
SELECT
    p1.product_name as product_a_name,
    p1.product_code as product_a_code,
    pc1.category_name as category_a,
    p2.product_name as product_b_name,
    p2.product_code as product_b_code,
    pc2.category_name as category_b,
    oc.times_bought_together,
    oc.orders_with_both,
    -- Calculate support (how often items appear together vs total orders)
    ROUND(
        oc.orders_with_both::NUMERIC /
        (SELECT COUNT(DISTINCT order_id) FROM sales_orders WHERE order_status NOT IN ('Cancelled', 'Returned')) * 100,
        4
    ) as support_pct
FROM order_combinations oc
JOIN products p1 ON oc.product_a = p1.product_id
JOIN products p2 ON oc.product_b = p2.product_id
JOIN product_categories pc1 ON p1.category_id = pc1.category_id
JOIN product_categories pc2 ON p2.category_id = pc2.category_id
ORDER BY times_bought_together DESC, support_pct DESC;

-- ===================================================================
-- 4. MARKETING CAMPAIGN EFFECTIVENESS
-- ===================================================================

-- Q7: Marketing campaign ROI and customer acquisition analysis
-- Test: "ROI campaign marketing mana yang paling tinggi?"
WITH campaign_performance AS (
    SELECT
        mc.campaign_id,
        mc.campaign_name,
        mc.campaign_type,
        mc.start_date,
        mc.end_date,
        mc.budget,
        -- Count customers acquired during campaign period
        COUNT(DISTINCT c.customer_id) as customers_acquired,
        -- Revenue from customers acquired during campaign
        SUM(so.total_amount) as revenue_from_acquired_customers,
        -- Calculate average metrics
        AVG(so.total_amount) as avg_order_value,
        COUNT(DISTINCT so.order_id) as total_orders_from_acquired
    FROM marketing_campaigns mc
    LEFT JOIN customers c ON c.registration_date BETWEEN mc.start_date AND mc.end_date
    LEFT JOIN sales_orders so ON c.customer_id = so.customer_id
        AND so.order_date BETWEEN mc.start_date AND (mc.end_date + INTERVAL '30 days')
        AND so.order_status NOT IN ('Cancelled', 'Returned')
    WHERE mc.status IN ('Completed', 'Active')
    GROUP BY mc.campaign_id, mc.campaign_name, mc.campaign_type,
             mc.start_date, mc.end_date, mc.budget
)
SELECT
    *,
    CASE
        WHEN customers_acquired > 0 THEN ROUND(budget / customers_acquired, 0)
        ELSE NULL
    END as cost_per_acquisition,
    CASE
        WHEN budget > 0 THEN ROUND((revenue_from_acquired_customers - budget) / budget * 100, 2)
        ELSE NULL
    END as roi_percentage,
    CASE
        WHEN customers_acquired > 0 THEN ROUND(revenue_from_acquired_customers / customers_acquired, 0)
        ELSE NULL
    END as revenue_per_customer
FROM campaign_performance
ORDER BY roi_percentage DESC NULLS LAST;

-- ===================================================================
-- 5. PREDICTIVE ANALYTICS & FORECASTING QUERIES
-- ===================================================================

-- Q8: Customer churn prediction based on behavior patterns
-- Test: "Customer mana yang berisiko churn dalam 3 bulan ke depan?"
WITH customer_behavior_metrics AS (
    SELECT
        c.customer_id,
        c.customer_code,
        c.first_name || ' ' || c.last_name as full_name,
        c.segment_id,
        c.registration_date,
        COUNT(DISTINCT so.order_id) as total_orders,
        SUM(so.total_amount) as lifetime_value,
        MAX(so.order_date) as last_order_date,
        EXTRACT(DAYS FROM NOW() - MAX(so.order_date)) as days_since_last_order,
        -- Calculate order frequency
        CASE
            WHEN COUNT(DISTINCT so.order_id) > 1 THEN
                EXTRACT(DAYS FROM MAX(so.order_date) - MIN(so.order_date)) /
                NULLIF(COUNT(DISTINCT so.order_id) - 1, 0)
            ELSE NULL
        END as avg_days_between_orders,
        -- Recent activity trend
        COUNT(DISTINCT CASE WHEN so.order_date >= NOW() - INTERVAL '3 months' THEN so.order_id END) as orders_last_3months,
        COUNT(DISTINCT CASE WHEN so.order_date >= NOW() - INTERVAL '6 months' THEN so.order_id END) as orders_last_6months,
        -- Support interactions
        (SELECT COUNT(*) FROM support_tickets st WHERE st.customer_id = c.customer_id
         AND st.created_at >= NOW() - INTERVAL '6 months') as support_tickets_6months
    FROM customers c
    LEFT JOIN sales_orders so ON c.customer_id = so.customer_id
        AND so.order_status NOT IN ('Cancelled', 'Returned')
    WHERE c.is_active = true AND c.total_orders > 0
    GROUP BY c.customer_id, c.customer_code, c.first_name, c.last_name,
             c.segment_id, c.registration_date
)
SELECT
    *,
    -- Churn risk scoring
    CASE
        WHEN days_since_last_order > 180 AND avg_days_between_orders < 90 THEN 'HIGH RISK'
        WHEN days_since_last_order > 120 AND orders_last_3months = 0 THEN 'MEDIUM-HIGH RISK'
        WHEN days_since_last_order > 90 AND orders_last_6months <= 1 THEN 'MEDIUM RISK'
        WHEN days_since_last_order > 60 AND support_tickets_6months >= 2 THEN 'MEDIUM RISK'
        WHEN days_since_last_order <= 30 THEN 'LOW RISK'
        ELSE 'MEDIUM-LOW RISK'
    END as churn_risk_level,
    -- Recommended actions
    CASE
        WHEN days_since_last_order > 180 THEN 'Win-back campaign with special discount'
        WHEN days_since_last_order > 120 THEN 'Personalized re-engagement email'
        WHEN days_since_last_order > 90 THEN 'Product recommendation based on history'
        WHEN support_tickets_6months >= 2 THEN 'Proactive customer service outreach'
        ELSE 'Continue regular engagement'
    END as recommended_action
FROM customer_behavior_metrics
WHERE total_orders >= 2  -- Focus on repeat customers
ORDER BY
    CASE churn_risk_level
        WHEN 'HIGH RISK' THEN 1
        WHEN 'MEDIUM-HIGH RISK' THEN 2
        WHEN 'MEDIUM RISK' THEN 3
        WHEN 'MEDIUM-LOW RISK' THEN 4
        WHEN 'LOW RISK' THEN 5
    END,
    lifetime_value DESC;

-- Q9: Sales forecasting using linear trend analysis
-- Test: "Prediksi penjualan 3 bulan ke depan berdasarkan trend historical"
WITH monthly_sales_trend AS (
    SELECT
        year,
        month,
        total_revenue,
        total_orders,
        ROW_NUMBER() OVER (ORDER BY year, month) as period_number
    FROM monthly_metrics
    WHERE year = 2024
),
trend_analysis AS (
    SELECT
        AVG(total_revenue) as avg_monthly_revenue,
        -- Simple linear regression for trend
        REGR_SLOPE(total_revenue, period_number) as revenue_trend_slope,
        REGR_INTERCEPT(total_revenue, period_number) as revenue_trend_intercept,
        REGR_SLOPE(total_orders, period_number) as orders_trend_slope,
        REGR_INTERCEPT(total_orders, period_number) as orders_trend_intercept,
        MAX(period_number) as last_period
    FROM monthly_sales_trend
)
SELECT
    'Forecast' as data_type,
    2025 as year,
    forecast_month as month,
    ROUND(ta.revenue_trend_intercept + (ta.revenue_trend_slope * (ta.last_period + forecast_month)), 0) as predicted_revenue,
    ROUND(ta.orders_trend_intercept + (ta.orders_trend_slope * (ta.last_period + forecast_month)), 0) as predicted_orders,
    CASE
        WHEN ta.revenue_trend_slope > 0 THEN 'Growing'
        WHEN ta.revenue_trend_slope < 0 THEN 'Declining'
        ELSE 'Stable'
    END as trend_direction,
    ROUND(ta.revenue_trend_slope / ta.avg_monthly_revenue * 100, 2) as monthly_growth_rate_pct
FROM trend_analysis ta
CROSS JOIN generate_series(1, 3) as forecast_month
UNION ALL
SELECT
    'Historical' as data_type,
    mst.year,
    mst.month,
    mst.total_revenue as predicted_revenue,
    mst.total_orders as predicted_orders,
    'Actual' as trend_direction,
    NULL as monthly_growth_rate_pct
FROM monthly_sales_trend mst
ORDER BY year, month;

-- ===================================================================
-- 6. OPERATIONAL EFFICIENCY QUERIES
-- ===================================================================

-- Q10: Warehouse efficiency and inventory distribution analysis
-- Test: "Efisiensi gudang dan distribusi stok optimal"
WITH warehouse_metrics AS (
    SELECT
        w.warehouse_id,
        w.warehouse_code,
        w.warehouse_name,
        w.city,
        w.capacity_cubic_meters,
        w.operational_cost_per_month,
        COUNT(DISTINCT pi.product_id) as products_stored,
        SUM(pi.current_stock) as total_stock_units,
        SUM(pi.current_stock * p.base_price) as total_inventory_value,
        AVG(pi.current_stock) as avg_stock_per_product,
        -- Calculate orders fulfilled from this warehouse (approximation)
        COUNT(DISTINCT so.order_id) as orders_fulfilled_estimate
    FROM warehouses w
    LEFT JOIN product_inventory pi ON w.warehouse_id = pi.warehouse_id
    LEFT JOIN products p ON pi.product_id = p.product_id
    LEFT JOIN sales_orders so ON so.shipping_city = w.city  -- Simple approximation
        AND so.order_status = 'Delivered'
        AND so.order_date >= '2024-01-01'
    WHERE w.is_active = true
    GROUP BY w.warehouse_id, w.warehouse_code, w.warehouse_name, w.city,
             w.capacity_cubic_meters, w.operational_cost_per_month
)
SELECT
    *,
    ROUND(total_inventory_value / operational_cost_per_month, 2) as inventory_to_cost_ratio,
    ROUND(orders_fulfilled_estimate / NULLIF(operational_cost_per_month, 0) * 1000000, 2) as orders_per_million_cost,
    ROUND(total_stock_units / capacity_cubic_meters, 2) as utilization_density,
    CASE
        WHEN inventory_to_cost_ratio > 100 THEN 'High Efficiency'
        WHEN inventory_to_cost_ratio > 50 THEN 'Medium Efficiency'
        ELSE 'Low Efficiency - Review Required'
    END as efficiency_rating
FROM warehouse_metrics
ORDER BY inventory_to_cost_ratio DESC;

-- ===================================================================
-- 7. ADVANCED BUSINESS INTELLIGENCE QUERIES
-- ===================================================================

-- Q11: Customer cohort analysis for retention insights
-- Test: "Analisis kohort customer untuk melihat retention rate"
WITH customer_cohorts AS (
    SELECT
        c.customer_id,
        DATE_TRUNC('month', c.registration_date) as cohort_month,
        DATE_TRUNC('month', so.order_date) as order_month,
        EXTRACT(MONTH FROM AGE(so.order_date, c.registration_date)) as months_since_registration
    FROM customers c
    LEFT JOIN sales_orders so ON c.customer_id = so.customer_id
        AND so.order_status NOT IN ('Cancelled', 'Returned')
    WHERE c.registration_date >= '2024-01-01'
),
cohort_sizes AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT customer_id) as cohort_size
    FROM customer_cohorts
    GROUP BY cohort_month
),
cohort_retention AS (
    SELECT
        cc.cohort_month,
        cc.months_since_registration,
        COUNT(DISTINCT cc.customer_id) as customers_active,
        cs.cohort_size
    FROM customer_cohorts cc
    JOIN cohort_sizes cs ON cc.cohort_month = cs.cohort_month
    WHERE cc.order_month IS NOT NULL
    GROUP BY cc.cohort_month, cc.months_since_registration, cs.cohort_size
)
SELECT
    cohort_month,
    months_since_registration,
    customers_active,
    cohort_size,
    ROUND(customers_active::NUMERIC / cohort_size * 100, 2) as retention_rate_pct,
    CASE
        WHEN months_since_registration = 0 THEN 'Initial Purchase'
        WHEN months_since_registration = 1 THEN '1 Month'
        WHEN months_since_registration <= 3 THEN '2-3 Months'
        WHEN months_since_registration <= 6 THEN '4-6 Months'
        ELSE '6+ Months'
    END as period_label
FROM cohort_retention
WHERE months_since_registration <= 12
ORDER BY cohort_month, months_since_registration;

-- Q12: Price elasticity and demand analysis
-- Test: "Analisis elastisitas harga untuk optimasi pricing"
WITH price_demand_analysis AS (
    SELECT
        p.product_id,
        p.product_name,
        pc.category_name,
        DATE_TRUNC('month', so.order_date) as sales_month,
        AVG(soi.unit_price) as avg_price,
        SUM(soi.quantity) as total_quantity_sold,
        COUNT(DISTINCT so.order_id) as order_frequency,
        SUM(soi.line_total) as total_revenue
    FROM products p
    JOIN sales_order_items soi ON p.product_id = soi.product_id
    JOIN sales_orders so ON soi.order_id = so.order_id
    JOIN product_categories pc ON p.category_id = pc.category_id
    WHERE so.order_status NOT IN ('Cancelled', 'Returned')
        AND so.order_date >= '2024-01-01'
    GROUP BY p.product_id, p.product_name, pc.category_name,
             DATE_TRUNC('month', so.order_date)
    HAVING COUNT(DISTINCT so.order_id) >= 2  -- Products with multiple orders
),
price_elasticity AS (
    SELECT
        product_id,
        product_name,
        category_name,
        COUNT(*) as months_with_sales,
        AVG(avg_price) as overall_avg_price,
        STDDEV(avg_price) as price_variance,
        AVG(total_quantity_sold) as avg_monthly_quantity,
        STDDEV(total_quantity_sold) as quantity_variance,
        -- Simple correlation between price and quantity
        CORR(avg_price, total_quantity_sold) as price_quantity_correlation
    FROM price_demand_analysis
    GROUP BY product_id, product_name, category_name
    HAVING COUNT(*) >= 3  -- At least 3 months of data
)
SELECT
    *,
    CASE
        WHEN price_quantity_correlation < -0.3 THEN 'Elastic (Price Sensitive)'
        WHEN price_quantity_correlation > 0.3 THEN 'Luxury/Premium (Price Insensitive)'
        ELSE 'Neutral Elasticity'
    END as elasticity_category,
    CASE
        WHEN price_quantity_correlation < -0.3 THEN 'Consider price optimization/discounts'
        WHEN price_quantity_correlation > 0.3 THEN 'Potential for price increases'
        ELSE 'Maintain current pricing strategy'
    END as pricing_recommendation,
    ROUND(price_variance / NULLIF(overall_avg_price, 0) * 100, 2) as price_volatility_pct
FROM price_elasticity
ORDER BY ABS(price_quantity_correlation) DESC;

-- ===================================================================
-- TESTING QUERY EXAMPLES FOR ATABOT
-- Natural language queries to test with the above data
-- ===================================================================

/*
SAMPLE NATURAL LANGUAGE QUERIES FOR TESTING:

1. SALES ANALYSIS:
   - "Bagaimana trend penjualan bulan ini dibanding bulan lalu?"
   - "Produk apa yang paling laku bulan ini?"
   - "Berapa total revenue Q4 2024?"
   - "Channel mana yang menghasilkan penjualan terbanyak?"

2. CUSTOMER INSIGHTS:
   - "Siapa customer dengan lifetime value tertinggi?"
   - "Berapa rata-rata order value per customer segment?"
   - "Customer dari kota mana yang paling banyak belanja?"
   - "Customer mana yang berisiko churn?"

3. PRODUCT PERFORMANCE:
   - "Produk mana yang profit marginnya paling tinggi?"
   - "Stok produk mana yang perlu direstok urgent?"
   - "Brand apa yang performa penjualannya paling baik?"
   - "Kategori produk apa yang seasonal trendnya paling kuat?"

4. INVENTORY MANAGEMENT:
   - "Gudang mana yang paling efisien dari segi cost?"
   - "Produk apa yang slow moving dan excess stock?"
   - "Inventory turnover ratio tertinggi untuk produk apa?"

5. MARKETING EFFECTIVENESS:
   - "Campaign marketing mana yang ROI-nya paling tinggi?"
   - "Channel acquisition customer mana yang paling cost effective?"
   - "Berapa customer acquisition cost rata-rata per channel?"

6. PREDICTIVE QUESTIONS:
   - "Prediksi penjualan 3 bulan ke depan berdasarkan historical data"
   - "Customer segment mana yang growth potentialnya paling tinggi?"
   - "Produk apa yang kemungkinan akan trending bulan depan?"

7. CROSS-SELLING OPPORTUNITIES:
   - "Produk apa yang sering dibeli bersamaan dengan iPhone?"
   - "Kombinasi produk mana yang menghasilkan basket value tertinggi?"
   - "Recommendation engine untuk customer yang beli laptop gaming"

8. OPERATIONAL EFFICIENCY:
   - "Rata-rata delivery time per kota/provinsi"
   - "Tingkat kepuasan customer berdasarkan rating dan review"
   - "Support ticket category mana yang paling sering muncul?"

9. FINANCIAL ANALYSIS:
   - "Gross profit margin per kategori produk"
   - "Break-even analysis untuk new product launch"
   - "Cost structure analysis per sales channel"

10. ADVANCED ANALYTICS:
    - "Customer segmentation berdasarkan RFM analysis"
    - "Price elasticity analysis untuk category electronics"
    - "Cohort analysis retention rate customer"
    - "Seasonal demand forecasting per product category"
*/

COMMENT ON DATABASE "atabot_testing" IS 'Comprehensive E-commerce Testing Database with 15+ tables, 1000+ records, complex relationships, and advanced analytics capabilities. Designed to test ATABOT AI system with realistic business scenarios including sales analysis, customer segmentation, inventory optimization, marketing ROI, predictive analytics, and operational efficiency queries.';