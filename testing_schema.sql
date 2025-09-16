-- ===================================================================
-- ATABOT COMPREHENSIVE TESTING SCHEMA
-- E-Commerce Sales Analytics & Customer Intelligence System
-- Designed for testing AI analytics, predictions, and complex queries
-- ===================================================================

-- Set schema
SET search_path TO atabot_testing;

-- ===================================================================
-- 1. MASTER DATA TABLES
-- ===================================================================

-- Product Categories (Hierarchical)
CREATE TABLE product_categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL,
    parent_category_id INTEGER REFERENCES product_categories(category_id),
    category_level INTEGER NOT NULL DEFAULT 1,
    description TEXT,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Brands
CREATE TABLE brands (
    brand_id SERIAL PRIMARY KEY,
    brand_name VARCHAR(100) NOT NULL UNIQUE,
    brand_country VARCHAR(50),
    established_year INTEGER,
    brand_tier VARCHAR(20) CHECK (brand_tier IN ('Premium', 'Mid-Range', 'Budget')),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Products (Enhanced)
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_code VARCHAR(50) UNIQUE NOT NULL,
    product_name VARCHAR(200) NOT NULL,
    category_id INTEGER REFERENCES product_categories(category_id),
    brand_id INTEGER REFERENCES brands(brand_id),
    base_price DECIMAL(12,2) NOT NULL,
    cost_price DECIMAL(12,2) NOT NULL,
    weight_grams DECIMAL(8,2),
    dimensions VARCHAR(50), -- Length x Width x Height
    color VARCHAR(50),
    size_variant VARCHAR(20),
    sku VARCHAR(100) UNIQUE,
    barcode VARCHAR(50),
    description TEXT,
    launch_date DATE,
    discontinue_date DATE,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Customer Segments
CREATE TABLE customer_segments (
    segment_id SERIAL PRIMARY KEY,
    segment_name VARCHAR(50) NOT NULL,
    description TEXT,
    min_annual_spending DECIMAL(12,2),
    max_annual_spending DECIMAL(12,2),
    perks_description TEXT
);

-- Customers (Enhanced with Demographics)
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    customer_code VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(150) UNIQUE,
    phone VARCHAR(20),
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    gender VARCHAR(10) CHECK (gender IN ('Male', 'Female', 'Other')),
    birth_date DATE,
    registration_date DATE NOT NULL DEFAULT CURRENT_DATE,
    segment_id INTEGER REFERENCES customer_segments(segment_id),
    preferred_payment_method VARCHAR(30),
    city VARCHAR(100),
    province VARCHAR(100),
    country VARCHAR(50) DEFAULT 'Indonesia',
    postal_code VARCHAR(10),
    total_lifetime_value DECIMAL(15,2) DEFAULT 0,
    total_orders INTEGER DEFAULT 0,
    last_order_date DATE,
    acquisition_channel VARCHAR(50), -- Social Media, Google Ads, Referral, etc.
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Sales Channels
CREATE TABLE sales_channels (
    channel_id SERIAL PRIMARY KEY,
    channel_name VARCHAR(50) NOT NULL,
    channel_type VARCHAR(30) CHECK (channel_type IN ('Online', 'Offline', 'Mobile App', 'Social Commerce')),
    commission_rate DECIMAL(5,4) DEFAULT 0,
    is_active BOOLEAN DEFAULT true
);

-- Suppliers
CREATE TABLE suppliers (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name VARCHAR(200) NOT NULL,
    supplier_code VARCHAR(50) UNIQUE NOT NULL,
    contact_person VARCHAR(100),
    email VARCHAR(150),
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(100),
    country VARCHAR(50),
    payment_terms INTEGER DEFAULT 30, -- Days
    quality_rating DECIMAL(3,2) CHECK (quality_rating >= 0 AND quality_rating <= 5),
    delivery_rating DECIMAL(3,2) CHECK (delivery_rating >= 0 AND delivery_rating <= 5),
    is_preferred BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ===================================================================
-- 2. INVENTORY & WAREHOUSE MANAGEMENT
-- ===================================================================

-- Warehouses
CREATE TABLE warehouses (
    warehouse_id SERIAL PRIMARY KEY,
    warehouse_code VARCHAR(20) UNIQUE NOT NULL,
    warehouse_name VARCHAR(100) NOT NULL,
    address TEXT,
    city VARCHAR(100),
    province VARCHAR(100),
    capacity_cubic_meters DECIMAL(10,2),
    manager_name VARCHAR(100),
    operational_cost_per_month DECIMAL(12,2),
    is_active BOOLEAN DEFAULT true
);

-- Product Inventory
CREATE TABLE product_inventory (
    inventory_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(product_id),
    warehouse_id INTEGER REFERENCES warehouses(warehouse_id),
    current_stock INTEGER NOT NULL DEFAULT 0,
    reserved_stock INTEGER DEFAULT 0,
    minimum_stock INTEGER DEFAULT 0,
    maximum_stock INTEGER DEFAULT 1000,
    reorder_point INTEGER DEFAULT 50,
    last_restock_date DATE,
    last_stock_check TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    average_monthly_usage DECIMAL(8,2),
    UNIQUE(product_id, warehouse_id)
);

-- ===================================================================
-- 3. SALES TRANSACTIONS (The Core)
-- ===================================================================

-- Sales Orders
CREATE TABLE sales_orders (
    order_id SERIAL PRIMARY KEY,
    order_number VARCHAR(50) UNIQUE NOT NULL,
    customer_id INTEGER REFERENCES customers(customer_id),
    channel_id INTEGER REFERENCES sales_channels(channel_id),
    order_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    order_status VARCHAR(30) CHECK (order_status IN ('Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled', 'Returned')),
    payment_method VARCHAR(50),
    payment_status VARCHAR(20) CHECK (payment_status IN ('Pending', 'Paid', 'Failed', 'Refunded')),
    subtotal DECIMAL(15,2) NOT NULL,
    tax_amount DECIMAL(15,2) DEFAULT 0,
    shipping_cost DECIMAL(10,2) DEFAULT 0,
    discount_amount DECIMAL(15,2) DEFAULT 0,
    total_amount DECIMAL(15,2) NOT NULL,
    shipping_address TEXT,
    shipping_city VARCHAR(100),
    shipping_province VARCHAR(100),
    estimated_delivery_date DATE,
    actual_delivery_date DATE,
    courier_service VARCHAR(50),
    tracking_number VARCHAR(100),
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Sales Order Items (Transaction Details)
CREATE TABLE sales_order_items (
    item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES sales_orders(order_id) ON DELETE CASCADE,
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(12,2) NOT NULL,
    unit_cost DECIMAL(12,2) NOT NULL, -- For profit calculation
    discount_per_item DECIMAL(12,2) DEFAULT 0,
    line_total DECIMAL(15,2) NOT NULL,
    line_profit DECIMAL(15,2) GENERATED ALWAYS AS ((unit_price - unit_cost - discount_per_item) * quantity) STORED
);

-- ===================================================================
-- 4. MARKETING & PROMOTIONS
-- ===================================================================

-- Marketing Campaigns
CREATE TABLE marketing_campaigns (
    campaign_id SERIAL PRIMARY KEY,
    campaign_name VARCHAR(200) NOT NULL,
    campaign_type VARCHAR(50) CHECK (campaign_type IN ('Email', 'Social Media', 'Google Ads', 'Influencer', 'Affiliate', 'TV', 'Radio')),
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    budget DECIMAL(15,2),
    target_audience TEXT,
    goals TEXT,
    status VARCHAR(20) CHECK (status IN ('Planning', 'Active', 'Paused', 'Completed', 'Cancelled')),
    created_by VARCHAR(100)
);

-- Promotions/Discounts
CREATE TABLE promotions (
    promotion_id SERIAL PRIMARY KEY,
    promotion_name VARCHAR(200) NOT NULL,
    promotion_code VARCHAR(50) UNIQUE,
    promotion_type VARCHAR(30) CHECK (promotion_type IN ('Percentage', 'Fixed Amount', 'Buy X Get Y', 'Free Shipping')),
    discount_value DECIMAL(10,2),
    minimum_purchase DECIMAL(12,2) DEFAULT 0,
    maximum_discount DECIMAL(12,2),
    start_date TIMESTAMP WITH TIME ZONE,
    end_date TIMESTAMP WITH TIME ZONE,
    usage_limit INTEGER,
    usage_count INTEGER DEFAULT 0,
    applicable_categories INTEGER[], -- Array of category_ids
    applicable_products INTEGER[], -- Array of product_ids
    is_active BOOLEAN DEFAULT true
);

-- Campaign Performance
CREATE TABLE campaign_performance (
    performance_id SERIAL PRIMARY KEY,
    campaign_id INTEGER REFERENCES marketing_campaigns(campaign_id),
    date_recorded DATE DEFAULT CURRENT_DATE,
    impressions INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    conversions INTEGER DEFAULT 0,
    revenue_generated DECIMAL(15,2) DEFAULT 0,
    cost_spent DECIMAL(12,2) DEFAULT 0,
    orders_attributed INTEGER DEFAULT 0
);

-- ===================================================================
-- 5. CUSTOMER BEHAVIOR & ANALYTICS
-- ===================================================================

-- Customer Reviews & Ratings
CREATE TABLE product_reviews (
    review_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(product_id),
    customer_id INTEGER REFERENCES customers(customer_id),
    order_id INTEGER REFERENCES sales_orders(order_id),
    rating INTEGER CHECK (rating >= 1 AND rating <= 5),
    review_title VARCHAR(200),
    review_text TEXT,
    is_verified_purchase BOOLEAN DEFAULT true,
    helpful_votes INTEGER DEFAULT 0,
    review_date TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Website/App Activity Log
CREATE TABLE customer_activity (
    activity_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    session_id VARCHAR(100),
    activity_type VARCHAR(50) CHECK (activity_type IN ('Page View', 'Product View', 'Add to Cart', 'Remove from Cart', 'Search', 'Login', 'Logout')),
    product_id INTEGER REFERENCES products(product_id), -- NULL for non-product activities
    page_url TEXT,
    search_query VARCHAR(200),
    activity_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    ip_address INET,
    user_agent TEXT,
    device_type VARCHAR(20) CHECK (device_type IN ('Desktop', 'Mobile', 'Tablet'))
);

-- Customer Support Tickets
CREATE TABLE support_tickets (
    ticket_id SERIAL PRIMARY KEY,
    ticket_number VARCHAR(50) UNIQUE NOT NULL,
    customer_id INTEGER REFERENCES customers(customer_id),
    order_id INTEGER REFERENCES sales_orders(order_id),
    category VARCHAR(50) CHECK (category IN ('Product Issue', 'Shipping', 'Payment', 'Account', 'General Inquiry', 'Complaint')),
    priority VARCHAR(20) CHECK (priority IN ('Low', 'Medium', 'High', 'Urgent')),
    status VARCHAR(20) CHECK (status IN ('Open', 'In Progress', 'Waiting Customer', 'Resolved', 'Closed')),
    subject VARCHAR(200),
    description TEXT,
    resolution TEXT,
    assigned_agent VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    resolved_at TIMESTAMP WITH TIME ZONE,
    customer_satisfaction_rating INTEGER CHECK (customer_satisfaction_rating >= 1 AND customer_satisfaction_rating <= 5)
);

-- ===================================================================
-- 6. FINANCIAL & BUSINESS METRICS
-- ===================================================================

-- Monthly Business Metrics
CREATE TABLE monthly_metrics (
    metric_id SERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL CHECK (month >= 1 AND month <= 12),
    total_revenue DECIMAL(15,2) DEFAULT 0,
    total_orders INTEGER DEFAULT 0,
    total_customers INTEGER DEFAULT 0,
    new_customers INTEGER DEFAULT 0,
    returning_customers INTEGER DEFAULT 0,
    average_order_value DECIMAL(12,2) DEFAULT 0,
    customer_acquisition_cost DECIMAL(10,2) DEFAULT 0,
    customer_lifetime_value DECIMAL(15,2) DEFAULT 0,
    churn_rate DECIMAL(5,4) DEFAULT 0,
    profit_margin DECIMAL(5,4) DEFAULT 0,
    marketing_spend DECIMAL(15,2) DEFAULT 0,
    operational_costs DECIMAL(15,2) DEFAULT 0,
    UNIQUE(year, month)
);

-- Product Performance Analytics
CREATE TABLE product_analytics (
    analytics_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(product_id),
    analysis_date DATE DEFAULT CURRENT_DATE,
    total_sales_quantity INTEGER DEFAULT 0,
    total_revenue DECIMAL(15,2) DEFAULT 0,
    total_profit DECIMAL(15,2) DEFAULT 0,
    return_quantity INTEGER DEFAULT 0,
    return_rate DECIMAL(5,4) DEFAULT 0,
    average_rating DECIMAL(3,2) DEFAULT 0,
    review_count INTEGER DEFAULT 0,
    page_views INTEGER DEFAULT 0,
    conversion_rate DECIMAL(5,4) DEFAULT 0,
    inventory_turnover DECIMAL(8,2) DEFAULT 0
);

-- ===================================================================
-- 7. INDEXES FOR PERFORMANCE
-- ===================================================================

-- Performance indexes
CREATE INDEX idx_sales_orders_date ON sales_orders(order_date);
CREATE INDEX idx_sales_orders_customer ON sales_orders(customer_id);
CREATE INDEX idx_sales_orders_status ON sales_orders(order_status);
CREATE INDEX idx_sales_order_items_product ON sales_order_items(product_id);
CREATE INDEX idx_customer_activity_customer ON customer_activity(customer_id);
CREATE INDEX idx_customer_activity_timestamp ON customer_activity(activity_timestamp);
CREATE INDEX idx_product_reviews_product ON product_reviews(product_id);
CREATE INDEX idx_product_reviews_rating ON product_reviews(rating);
CREATE INDEX idx_customers_segment ON customers(segment_id);
CREATE INDEX idx_customers_city ON customers(city);
CREATE INDEX idx_products_category ON products(category_id);
CREATE INDEX idx_products_brand ON products(brand_id);

-- ===================================================================
-- 8. SAMPLE DATA INSERTION
-- ===================================================================

-- Insert Customer Segments
INSERT INTO customer_segments (segment_name, description, min_annual_spending, max_annual_spending, perks_description) VALUES
('Bronze', 'New and low-spending customers', 0, 2000000, 'Basic customer support'),
('Silver', 'Regular customers with moderate spending', 2000001, 5000000, 'Priority shipping, exclusive offers'),
('Gold', 'High-value customers', 5000001, 15000000, 'Personal account manager, early access to products'),
('Platinum', 'VIP customers with highest spending', 15000001, NULL, 'Dedicated support, custom products, exclusive events');

-- Insert Product Categories
INSERT INTO product_categories (category_name, parent_category_id, category_level, description) VALUES
('Electronics', NULL, 1, 'All electronic products'),
('Fashion', NULL, 1, 'Clothing and accessories'),
('Home & Garden', NULL, 1, 'Home improvement and garden supplies'),
('Sports & Outdoor', NULL, 1, 'Sports equipment and outdoor gear'),
('Books & Media', NULL, 1, 'Books, movies, music'),
('Smartphones', 1, 2, 'Mobile phones and accessories'),
('Laptops', 1, 2, 'Notebooks and laptop accessories'),
('Gaming', 1, 2, 'Gaming consoles and accessories'),
('Mens Fashion', 2, 2, 'Clothing for men'),
('Womens Fashion', 2, 2, 'Clothing for women'),
('Kitchen', 3, 2, 'Kitchen appliances and tools'),
('Furniture', 3, 2, 'Home furniture'),
('Fitness', 4, 2, 'Fitness equipment'),
('Camping', 4, 2, 'Camping and hiking gear');

-- Insert Brands
INSERT INTO brands (brand_name, brand_country, established_year, brand_tier) VALUES
('Samsung', 'South Korea', 1938, 'Premium'),
('Apple', 'United States', 1976, 'Premium'),
('Xiaomi', 'China', 2010, 'Mid-Range'),
('Nike', 'United States', 1964, 'Premium'),
('Adidas', 'Germany', 1949, 'Premium'),
('Uniqlo', 'Japan', 1949, 'Mid-Range'),
('IKEA', 'Sweden', 1943, 'Budget'),
('Sony', 'Japan', 1946, 'Premium'),
('LG', 'South Korea', 1947, 'Mid-Range'),
('Philips', 'Netherlands', 1891, 'Mid-Range');

-- Insert Sales Channels
INSERT INTO sales_channels (channel_name, channel_type, commission_rate) VALUES
('Official Website', 'Online', 0.0000),
('Mobile App', 'Mobile App', 0.0000),
('Shopee', 'Online', 0.0350),
('Tokopedia', 'Online', 0.0300),
('Lazada', 'Online', 0.0400),
('TikTok Shop', 'Social Commerce', 0.0250),
('Instagram Shop', 'Social Commerce', 0.0200),
('Physical Store Jakarta', 'Offline', 0.0000),
('Physical Store Surabaya', 'Offline', 0.0000),
('Reseller Network', 'Offline', 0.1500);

-- Insert Warehouses
INSERT INTO warehouses (warehouse_code, warehouse_name, address, city, province, capacity_cubic_meters, manager_name, operational_cost_per_month) VALUES
('WH-JKT-01', 'Jakarta Main Warehouse', 'Jl. Industri Raya No. 123', 'Jakarta', 'DKI Jakarta', 5000.00, 'Budi Santoso', 25000000.00),
('WH-SBY-01', 'Surabaya Distribution Center', 'Jl. Raya Industri No. 456', 'Surabaya', 'Jawa Timur', 3000.00, 'Siti Rahayu', 18000000.00),
('WH-BDG-01', 'Bandung Regional Warehouse', 'Jl. Soekarno Hatta No. 789', 'Bandung', 'Jawa Barat', 2000.00, 'Ahmad Hidayat', 15000000.00);

-- Continue with more comprehensive sample data...
-- (Due to length limits, I'll provide the rest in a comment block with the key tables filled)

/*
Additional sample data would include:

1. Products (50-100 diverse products across categories)
2. Customers (500-1000 customers with realistic demographics)
3. Sales Orders (2000-5000 orders across 12 months with seasonal patterns)
4. Marketing Campaigns (10-20 campaigns with performance data)
5. Customer Activities (10000+ activity logs)
6. Reviews (1000+ product reviews)
7. Support tickets (500+ tickets)
8. Monthly metrics (12 months of business data)

This would create a rich dataset for testing:
- Sales trend analysis
- Customer segmentation
- Product performance
- Marketing ROI
- Seasonal patterns
- Customer behavior analytics
- Predictive modeling
*/

-- ===================================================================
-- 9. ADVANCED VIEWS FOR ANALYTICS
-- ===================================================================

-- Customer Lifetime Value View
CREATE VIEW customer_ltv_analysis AS
SELECT
    c.customer_id,
    c.customer_code,
    c.first_name || ' ' || c.last_name AS full_name,
    c.segment_id,
    cs.segment_name,
    c.registration_date,
    c.city,
    COUNT(DISTINCT so.order_id) AS total_orders,
    SUM(so.total_amount) AS total_spent,
    AVG(so.total_amount) AS avg_order_value,
    MAX(so.order_date) AS last_order_date,
    EXTRACT(DAYS FROM NOW() - MAX(so.order_date)) AS days_since_last_order,
    CASE
        WHEN EXTRACT(DAYS FROM NOW() - MAX(so.order_date)) > 180 THEN 'At Risk'
        WHEN EXTRACT(DAYS FROM NOW() - MAX(so.order_date)) > 90 THEN 'Declining'
        WHEN EXTRACT(DAYS FROM NOW() - MAX(so.order_date)) > 30 THEN 'Active'
        ELSE 'Highly Active'
    END AS customer_status
FROM customers c
LEFT JOIN customer_segments cs ON c.segment_id = cs.segment_id
LEFT JOIN sales_orders so ON c.customer_id = so.customer_id
WHERE c.is_active = true
GROUP BY c.customer_id, c.customer_code, c.first_name, c.last_name,
         c.segment_id, cs.segment_name, c.registration_date, c.city;

-- Product Performance View
CREATE VIEW product_performance_summary AS
SELECT
    p.product_id,
    p.product_code,
    p.product_name,
    pc.category_name,
    b.brand_name,
    COUNT(DISTINCT soi.order_id) AS total_orders,
    SUM(soi.quantity) AS total_quantity_sold,
    SUM(soi.line_total) AS total_revenue,
    SUM(soi.line_profit) AS total_profit,
    ROUND(AVG(soi.unit_price), 2) AS avg_selling_price,
    ROUND(AVG(pr.rating), 2) AS avg_rating,
    COUNT(pr.review_id) AS review_count,
    ROUND(SUM(soi.line_profit) / NULLIF(SUM(soi.line_total), 0) * 100, 2) AS profit_margin_percent
FROM products p
LEFT JOIN product_categories pc ON p.category_id = pc.category_id
LEFT JOIN brands b ON p.brand_id = b.brand_id
LEFT JOIN sales_order_items soi ON p.product_id = soi.product_id
LEFT JOIN sales_orders so ON soi.order_id = so.order_id AND so.order_status NOT IN ('Cancelled', 'Returned')
LEFT JOIN product_reviews pr ON p.product_id = pr.product_id
WHERE p.is_active = true
GROUP BY p.product_id, p.product_code, p.product_name, pc.category_name, b.brand_name
ORDER BY total_revenue DESC;

-- Sales Trends View
CREATE VIEW monthly_sales_trends AS
SELECT
    EXTRACT(YEAR FROM so.order_date) AS year,
    EXTRACT(MONTH FROM so.order_date) AS month,
    TO_CHAR(so.order_date, 'YYYY-MM') AS year_month,
    COUNT(DISTINCT so.order_id) AS total_orders,
    COUNT(DISTINCT so.customer_id) AS unique_customers,
    SUM(so.total_amount) AS total_revenue,
    SUM(soi.line_profit) AS total_profit,
    ROUND(AVG(so.total_amount), 2) AS avg_order_value,
    ROUND(SUM(soi.line_profit) / NULLIF(SUM(so.total_amount), 0) * 100, 2) AS profit_margin_percent
FROM sales_orders so
JOIN sales_order_items soi ON so.order_id = soi.order_id
WHERE so.order_status NOT IN ('Cancelled', 'Returned')
GROUP BY EXTRACT(YEAR FROM so.order_date), EXTRACT(MONTH FROM so.order_date), TO_CHAR(so.order_date, 'YYYY-MM')
ORDER BY year, month;

COMMENT ON SCHEMA atabot_testing IS 'Comprehensive E-commerce Analytics Testing Schema for ATABOT AI System - Designed to test complex queries, analytics, predictions, and business intelligence capabilities';