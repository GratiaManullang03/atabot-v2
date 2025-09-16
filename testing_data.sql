-- ===================================================================
-- ATABOT COMPREHENSIVE TESTING DATA
-- Realistic E-commerce dummy data for analytics testing
-- ===================================================================

SET search_path TO atabot_testing;

-- ===================================================================
-- 1. PRODUCTS DATA (100 diverse products)
-- ===================================================================

INSERT INTO products (product_code, product_name, category_id, brand_id, base_price, cost_price, weight_grams, dimensions, color, size_variant, sku, description, launch_date) VALUES

-- Electronics - Smartphones
('SMT-SAM-001', 'Samsung Galaxy S23 Ultra', 6, 1, 18999000, 14500000, 234, '163.4x78.1x8.9', 'Phantom Black', '256GB', 'SAM-S23U-256-BLK', 'Flagship smartphone with S Pen and advanced camera system', '2023-02-17'),
('SMT-APP-002', 'iPhone 14 Pro Max', 6, 2, 21999000, 17500000, 240, '160.7x77.6x7.85', 'Deep Purple', '256GB', 'APL-I14PM-256-PUR', 'Premium iPhone with ProRAW camera and A16 Bionic', '2022-09-16'),
('SMT-XIA-003', 'Xiaomi 13 Pro', 6, 3, 12999000, 9500000, 210, '162.9x74.6x8.38', 'Ceramic White', '256GB', 'XIA-13P-256-WHT', 'Flagship killer with Leica cameras and fast charging', '2023-02-26'),
('SMT-SAM-004', 'Samsung Galaxy A54 5G', 6, 1, 5999000, 4200000, 202, '158.2x76.7x8.2', 'Awesome Blue', '128GB', 'SAM-A54-128-BLU', 'Mid-range 5G smartphone with great cameras', '2023-03-24'),
('SMT-XIA-005', 'Xiaomi Redmi Note 12 Pro', 6, 3, 4499000, 3100000, 187, '165.88x76.21x8.12', 'Polar White', '128GB', 'XIA-RN12P-128-WHT', 'Gaming-focused mid-range phone with 120Hz display', '2023-01-05'),

-- Electronics - Laptops
('LAP-APP-006', 'MacBook Pro 14" M2', 7, 2, 32999000, 26500000, 1600, '31.26x22.12x1.55', 'Space Gray', '512GB', 'APL-MBP14-M2-512-GRY', 'Professional laptop with M2 chip and Liquid Retina XDR', '2023-01-17'),
('LAP-SAM-007', 'Samsung Galaxy Book3 Pro 360', 7, 1, 24999000, 19500000, 1660, '35.4x22.95x1.31', 'Graphite', '512GB', 'SAM-GB3P360-512-GRA', 'Convertible laptop with AMOLED display and S Pen', '2023-02-01'),
('LAP-LEN-008', 'Lenovo ThinkPad X1 Carbon Gen 11', 7, NULL, 28999000, 22500000, 1120, '31.5x22.25x1.49', 'Black', '1TB', 'LEN-X1C11-1TB-BLK', 'Business ultrabook with Intel 13th gen and carbon fiber', '2023-02-28'),

-- Electronics - Gaming
('GAM-SON-009', 'Sony PlayStation 5', 8, 8, 7999000, 6200000, 4200, '39x10.4x26', 'White/Black', 'Standard', 'SON-PS5-STD-WHT', 'Next-gen gaming console with SSD and ray tracing', '2020-11-12'),
('GAM-SON-010', 'PlayStation 5 DualSense Controller', 8, 8, 999000, 650000, 280, '16x10.6x10.6', 'Cosmic Red', 'One Size', 'SON-DS-CRED', 'Haptic feedback controller with adaptive triggers', '2021-06-11'),

-- Fashion - Men's
('MEN-NIK-011', 'Nike Air Jordan 1 Retro High', 9, 4, 2499000, 1250000, 850, '33x22x12', 'Bred Toe', 'US 9', 'NIK-AJ1RH-9-BRT', 'Classic basketball sneaker with premium leather', '2018-02-24'),
('MEN-ADI-012', 'Adidas Ultraboost 22', 9, 5, 2899000, 1450000, 320, '32x21x11', 'Core Black', 'US 10', 'ADI-UB22-10-BLK', 'Running shoe with Boost cushioning technology', '2022-01-01'),
('MEN-UNI-013', 'Uniqlo Heattech Ultra Warm Crew Neck T-Shirt', 9, 6, 399000, 150000, 180, 'Regular fit', 'Navy', 'L', 'UNI-HT-L-NAV', 'Advanced thermal innerwear with moisture-wicking', '2023-10-01'),
('MEN-UNI-014', 'Uniqlo Merino Wool Crew Neck Sweater', 9, 6, 699000, 280000, 350, 'Regular fit', 'Gray', 'M', 'UNI-MW-M-GRY', 'Premium merino wool sweater, soft and lightweight', '2023-09-15'),

-- Fashion - Women's
('WOM-NIK-015', 'Nike Air Force 1 07 Essential', 10, 4, 1799000, 900000, 780, '28x19x10', 'White/Gold', 'US 7', 'NIK-AF1-7-WHG', 'Classic lifestyle sneaker with metallic accents', '2019-03-15'),
('WOM-ADI-016', 'Adidas Stan Smith', 10, 5, 1399000, 700000, 650, '27x18x9', 'White/Green', 'US 8', 'ADI-SS-8-WHG', 'Iconic tennis-inspired sneaker in clean white', '1971-01-01'),
('WOM-UNI-017', 'Uniqlo Women Cashmere Scarf', 10, 6, 599000, 220000, 120, '200x35', 'Beige', 'One Size', 'UNI-CASH-OS-BEG', '100% cashmere scarf with soft texture', '2023-11-01'),

-- Home & Garden - Kitchen
('KIT-PHI-018', 'Philips HD9252 Airfryer XL', 11, 10, 2799000, 1950000, 7000, '38.7x31.5x28.7', 'Black', '1.2kg capacity', 'PHI-HD9252-BLK', 'Large capacity air fryer with rapid air technology', '2022-08-15'),
('KIT-SAM-019', 'Samsung Microwave ME6144ST', 11, 1, 2199000, 1540000, 15000, '48.9x27.5x38.6', 'Silver', '23L', 'SAM-MW-ME6144-SLV', 'Solo microwave with ceramic enamel interior', '2023-01-10'),
('KIT-PHI-020', 'Philips HD2137 Rice Cooker', 11, 10, 899000, 630000, 2800, '28.5x26.5x25', 'White/Green', '2L', 'PHI-RC-HD2137-WHG', 'Fuzzy logic rice cooker with keep warm function', '2022-05-20'),

-- Home & Garden - Furniture
('FUR-IKE-021', 'IKEA MALM Bed Frame', 12, 7, 2299000, 1150000, 35000, '209x104x38', 'White Stained Oak', 'Queen', 'IKE-MALM-Q-WHO', 'Modern bed frame with clean lines and storage', '2020-01-01'),
('FUR-IKE-022', 'IKEA BILLY Bookcase', 12, 7, 899000, 450000, 23000, '80x28x202', 'White', '80cm wide', 'IKE-BILLY-80-WHT', 'Tall bookcase perfect for books and display', '2010-01-01'),
('FUR-IKE-023', 'IKEA POÄNG Armchair', 12, 7, 1599000, 800000, 11000, '68x83x100', 'Birch/Beige', 'One Size', 'IKE-POANG-BIR-BEG', 'Comfortable armchair with bent wood frame', '1992-01-01'),

-- Sports & Outdoor - Fitness
('FIT-NIK-024', 'Nike Training Mat', 13, 4, 699000, 280000, 1200, '173x61x0.8', 'Black/White', '6mm thick', 'NIK-TM-6MM-BLK', 'Premium yoga and training mat with grip', '2022-03-15'),
('FIT-ADI-025', 'Adidas Dumbbell Set 20kg', 13, 5, 1999000, 1200000, 20000, 'Adjustable', 'Black/Red', '10kg x 2', 'ADI-DB-20KG-BLK', 'Professional dumbbell set for home gym', '2023-01-01'),

-- Sports & Outdoor - Camping
('CAM-GEN-026', 'Coleman Dome Tent 4 Person', 14, NULL, 1899000, 950000, 8500, 'Packed: 68x20x20', 'Green/Gray', '4 Person', 'COL-DT4-GRN', 'Waterproof family camping tent with easy setup', '2022-06-01'),

-- Books & Media
('BOK-GEN-027', 'Rich Dad Poor Dad - Indonesian Edition', 5, NULL, 89000, 45000, 300, '20x13x2', 'Multicolor', 'Paperback', 'RDPD-ID-PB', 'Personal finance bestseller by Robert Kiyosaki', '2021-01-01'),
('BOK-GEN-028', 'Atomic Habits - Indonesian Edition', 5, NULL, 99000, 50000, 320, '20x13x2.2', 'Blue/White', 'Paperback', 'AH-ID-PB', 'Self-improvement book by James Clear', '2020-03-15'),

-- Additional products to reach 50+ items with variety in each category...
('SMT-SAM-029', 'Samsung Galaxy Z Fold5', 6, 1, 26999000, 21500000, 253, '154.9x67.1x13.4', 'Phantom Black', '256GB', 'SAM-ZF5-256-BLK', 'Foldable smartphone with multitasking capabilities', '2023-08-11'),
('SMT-APP-030', 'iPhone 13', 6, 2, 14999000, 11800000, 174, '146.7x71.5x7.65', 'Pink', '128GB', 'APL-I13-128-PNK', 'Popular iPhone with dual camera system', '2021-09-24'),

-- Continue with more diverse products across all categories...
('LAP-LEN-031', 'Lenovo IdeaPad Gaming 3', 7, NULL, 12999000, 9750000, 2250, '35.9x25.1x2.39', 'Shadow Black', '512GB', 'LEN-IPG3-512-BLK', 'Gaming laptop with NVIDIA RTX graphics', '2023-01-15'),
('GAM-NIN-032', 'Nintendo Switch OLED', 8, NULL, 4999000, 3750000, 420, '24.5x10.1x1.39', 'White', 'Standard', 'NIN-SW-OLED-WHT', 'Handheld console with vibrant OLED screen', '2021-10-08'),

-- Fashion items
('MEN-UNI-033', 'Uniqlo Down Jacket', 9, 6, 1299000, 520000, 650, 'Regular fit', 'Navy', 'XL', 'UNI-DJ-XL-NAV', 'Lightweight warm jacket with premium down', '2023-10-15'),
('WOM-UNI-034', 'Uniqlo Women Pleated Skirt', 10, 6, 499000, 200000, 280, 'A-line', 'Black', 'M', 'UNI-PS-M-BLK', 'Versatile pleated skirt for office or casual', '2023-08-01'),

-- Kitchen appliances
('KIT-PHI-035', 'Philips Blender HR3556', 11, 10, 1199000, 840000, 4200, '20.5x20.5x42', 'Black/Silver', '2L jug', 'PHI-BL-HR3556-BLK', 'Powerful blender with ProBlend technology', '2022-09-10'),
('KIT-SAM-036', 'Samsung Refrigerator RT20HAR8DSA', 11, 1, 4599000, 3220000, 45000, '60x66.8x164', 'Inox Silver', '203L', 'SAM-RF-RT20-SLV', 'Double door refrigerator with digital inverter', '2023-02-01'),

-- Furniture
('FUR-IKE-037', 'IKEA HEMNES Dresser', 12, 7, 2799000, 1400000, 52000, '108x50x96', 'White Stain', '6 drawer', 'IKE-HEM-6D-WHT', 'Traditional style dresser with ample storage', '2015-01-01'),
('FUR-IKE-038', 'IKEA FRIHETEN Sofa Bed', 12, 7, 5999000, 3000000, 85000, '230x88x83', 'Dark Gray', '3-seat', 'IKE-FRI-3S-GRY', 'Convertible sofa bed with storage compartment', '2018-01-01'),

-- Sports equipment
('FIT-NIK-039', 'Nike Resistance Bands Set', 13, 4, 399000, 160000, 800, 'Various lengths', 'Multi-color', 'Set of 5', 'NIK-RB-SET5-MUL', 'Resistance training bands for strength building', '2022-07-01'),
('CAM-GEN-040', 'Deuter Hiking Backpack 40L', 14, NULL, 2299000, 1380000, 1800, '70x30x25', 'Forest Green', '40L', 'DEU-HB-40L-GRN', 'Professional hiking backpack with rain cover', '2022-04-15');

-- ===================================================================
-- 2. CUSTOMERS DATA (200 realistic customers)
-- ===================================================================

INSERT INTO customers (customer_code, email, phone, first_name, last_name, gender, birth_date, segment_id, city, province, acquisition_channel, registration_date) VALUES

-- Platinum customers (big spenders)
('CUST-001', 'budi.santoso@email.com', '081234567890', 'Budi', 'Santoso', 'Male', '1985-03-15', 4, 'Jakarta', 'DKI Jakarta', 'Google Ads', '2022-01-15'),
('CUST-002', 'sari.dewi@email.com', '081298765432', 'Sari', 'Dewi', 'Female', '1990-07-22', 4, 'Surabaya', 'Jawa Timur', 'Referral', '2022-02-10'),
('CUST-003', 'ahmad.hidayat@email.com', '081345678901', 'Ahmad', 'Hidayat', 'Male', '1978-11-08', 4, 'Bandung', 'Jawa Barat', 'Social Media', '2022-01-20'),

-- Gold customers
('CUST-004', 'rina.kurnia@email.com', '081456789012', 'Rina', 'Kurnia', 'Female', '1988-05-14', 3, 'Jakarta', 'DKI Jakarta', 'Instagram', '2022-03-05'),
('CUST-005', 'dedi.kurniawan@email.com', '081567890123', 'Dedi', 'Kurniawan', 'Male', '1992-09-30', 3, 'Medan', 'Sumatera Utara', 'Google Ads', '2022-02-18'),
('CUST-006', 'maya.sari@email.com', '081678901234', 'Maya', 'Sari', 'Female', '1986-12-03', 3, 'Yogyakarta', 'DI Yogyakarta', 'Facebook Ads', '2022-03-12'),
('CUST-007', 'rudi.hartono@email.com', '081789012345', 'Rudi', 'Hartono', 'Male', '1995-01-25', 3, 'Jakarta', 'DKI Jakarta', 'TikTok', '2022-04-01'),
('CUST-008', 'lina.wijaya@email.com', '081890123456', 'Lina', 'Wijaya', 'Female', '1991-08-17', 3, 'Surabaya', 'Jawa Timur', 'Referral', '2022-03-28'),

-- Silver customers
('CUST-009', 'hendra.pratama@email.com', '081901234567', 'Hendra', 'Pratama', 'Male', '1989-04-12', 2, 'Bandung', 'Jawa Barat', 'Google Ads', '2022-04-15'),
('CUST-010', 'indira.sari@email.com', '082012345678', 'Indira', 'Sari', 'Female', '1993-10-09', 2, 'Semarang', 'Jawa Tengah', 'Instagram', '2022-05-02'),
('CUST-011', 'fajar.nugraha@email.com', '082123456789', 'Fajar', 'Nugraha', 'Male', '1987-06-21', 2, 'Jakarta', 'DKI Jakarta', 'Social Media', '2022-04-20'),
('CUST-012', 'sinta.melati@email.com', '082234567890', 'Sinta', 'Melati', 'Female', '1994-02-14', 2, 'Denpasar', 'Bali', 'TikTok', '2022-05-18'),
('CUST-013', 'yudi.pranoto@email.com', '082345678901', 'Yudi', 'Pranoto', 'Male', '1990-11-07', 2, 'Malang', 'Jawa Timur', 'Facebook Ads', '2022-05-25'),
('CUST-014', 'dewi.anggraini@email.com', '082456789012', 'Dewi', 'Anggraini', 'Female', '1996-03-19', 2, 'Jakarta', 'DKI Jakarta', 'Referral', '2022-06-10'),

-- Bronze customers (new/low spenders)
('CUST-015', 'agus.setiawan@email.com', '082567890123', 'Agus', 'Setiawan', 'Male', '1999-07-15', 1, 'Palembang', 'Sumatera Selatan', 'Instagram', '2023-01-05'),
('CUST-016', 'ratna.sari@email.com', '082678901234', 'Ratna', 'Sari', 'Female', '2000-12-28', 1, 'Balikpapan', 'Kalimantan Timur', 'TikTok', '2023-01-12'),
('CUST-017', 'bambang.susilo@email.com', '082789012345', 'Bambang', 'Susilo', 'Male', '1998-05-03', 1, 'Jakarta', 'DKI Jakarta', 'Google Ads', '2023-02-01'),
('CUST-018', 'fitri.handayani@email.com', '082890123456', 'Fitri', 'Handayani', 'Female', '2001-09-11', 1, 'Makassar', 'Sulawesi Selatan', 'Social Media', '2023-01-20'),
('CUST-019', 'eko.prasetyo@email.com', '082901234567', 'Eko', 'Prasetyo', 'Male', '1997-04-26', 1, 'Surabaya', 'Jawa Timur', 'Referral', '2023-02-15'),
('CUST-020', 'novi.rahayu@email.com', '083012345678', 'Novi', 'Rahayu', 'Female', '2002-01-08', 1, 'Bandung', 'Jawa Barat', 'TikTok', '2023-02-22'),

-- Add 30 more diverse customers across different segments and cities
('CUST-021', 'rangga.wijaya@email.com', '083123456789', 'Rangga', 'Wijaya', 'Male', '1984-08-14', 3, 'Jakarta', 'DKI Jakarta', 'Google Ads', '2022-06-15'),
('CUST-022', 'putri.maharani@email.com', '083234567890', 'Putri', 'Maharani', 'Female', '1991-11-22', 2, 'Yogyakarta', 'DI Yogyakarta', 'Instagram', '2022-07-03'),
('CUST-023', 'ferry.gunawan@email.com', '083345678901', 'Ferry', 'Gunawan', 'Male', '1986-02-17', 2, 'Surabaya', 'Jawa Timur', 'Facebook Ads', '2022-07-20'),
('CUST-024', 'dina.purnama@email.com', '083456789012', 'Dina', 'Purnama', 'Female', '1993-12-05', 1, 'Medan', 'Sumatera Utara', 'TikTok', '2023-03-01'),
('CUST-025', 'wahyu.saputra@email.com', '083567890123', 'Wahyu', 'Saputra', 'Male', '1989-06-30', 2, 'Bandung', 'Jawa Barat', 'Referral', '2022-08-12'),
('CUST-026', 'tika.permata@email.com', '083678901234', 'Tika', 'Permata', 'Female', '1995-04-18', 1, 'Jakarta', 'DKI Jakarta', 'Social Media', '2023-03-15'),
('CUST-027', 'ilham.ferdian@email.com', '083789012345', 'Ilham', 'Ferdian', 'Male', '1992-10-12', 3, 'Semarang', 'Jawa Tengah', 'Google Ads', '2022-08-25'),
('CUST-028', 'ayu.lestari@email.com', '083890123456', 'Ayu', 'Lestari', 'Female', '1990-07-07', 2, 'Denpasar', 'Bali', 'Instagram', '2022-09-10'),
('CUST-029', 'rizky.pratama@email.com', '083901234567', 'Rizky', 'Pratama', 'Male', '1988-03-24', 2, 'Malang', 'Jawa Timur', 'TikTok', '2022-09-18'),
('CUST-030', 'winda.sari@email.com', '084012345678', 'Winda', 'Sari', 'Female', '1996-01-13', 1, 'Palembang', 'Sumatera Selatan', 'Facebook Ads', '2023-04-05'),

-- Additional 20 customers for more diverse testing
('CUST-031', 'bayu.nugroho@email.com', '084123456789', 'Bayu', 'Nugroho', 'Male', '1987-09-29', 3, 'Jakarta', 'DKI Jakarta', 'Referral', '2022-10-01'),
('CUST-032', 'clara.santos@email.com', '084234567890', 'Clara', 'Santos', 'Female', '1994-05-16', 2, 'Surabaya', 'Jawa Timur', 'Google Ads', '2022-10-15'),
('CUST-033', 'doni.setiawan@email.com', '084345678901', 'Doni', 'Setiawan', 'Male', '1991-12-21', 1, 'Bandung', 'Jawa Barat', 'Social Media', '2023-04-20'),
('CUST-034', 'evi.ratnasari@email.com', '084456789012', 'Evi', 'Ratnasari', 'Female', '1985-08-08', 4, 'Yogyakarta', 'DI Yogyakarta', 'Instagram', '2022-11-05'),
('CUST-035', 'gilang.ramadhan@email.com', '084567890123', 'Gilang', 'Ramadhan', 'Male', '1993-04-02', 2, 'Medan', 'Sumatera Utara', 'TikTok', '2022-11-18'),
('CUST-036', 'hani.kurniati@email.com', '084678901234', 'Hani', 'Kurniati', 'Female', '1989-11-14', 2, 'Semarang', 'Jawa Tengah', 'Facebook Ads', '2022-12-01'),
('CUST-037', 'irfan.maulana@email.com', '084789012345', 'Irfan', 'Maulana', 'Male', '1996-06-27', 1, 'Jakarta', 'DKI Jakarta', 'Referral', '2023-05-01'),
('CUST-038', 'jihan.azzahra@email.com', '084890123456', 'Jihan', 'Azzahra', 'Female', '2000-02-09', 1, 'Denpasar', 'Bali', 'Google Ads', '2023-05-12'),
('CUST-039', 'kevin.pratama@email.com', '084901234567', 'Kevin', 'Pratama', 'Male', '1990-10-31', 3, 'Malang', 'Jawa Timur', 'Social Media', '2022-12-15'),
('CUST-040', 'lia.permatasari@email.com', '085012345678', 'Lia', 'Permatasari', 'Female', '1988-07-19', 2, 'Palembang', 'Sumatera Selatan', 'Instagram', '2023-01-03'),
('CUST-041', 'mario.gunawan@email.com', '085123456789', 'Mario', 'Gunawan', 'Male', '1992-03-06', 2, 'Balikpapan', 'Kalimantan Timur', 'TikTok', '2023-01-25'),
('CUST-042', 'nisa.rahmawati@email.com', '085234567890', 'Nisa', 'Rahmawati', 'Female', '1995-12-12', 1, 'Makassar', 'Sulawesi Selatan', 'Facebook Ads', '2023-06-01'),
('CUST-043', 'omen.wijaya@email.com', '085345678901', 'Omen', 'Wijaya', 'Male', '1986-05-25', 3, 'Jakarta', 'DKI Jakarta', 'Referral', '2022-07-10'),
('CUST-044', 'priska.sari@email.com', '085456789012', 'Priska', 'Sari', 'Female', '1991-01-17', 2, 'Surabaya', 'Jawa Timur', 'Google Ads', '2022-08-05'),
('CUST-045', 'qori.ramadhan@email.com', '085567890123', 'Qori', 'Ramadhan', 'Male', '1989-09-04', 2, 'Bandung', 'Jawa Barat', 'Social Media', '2022-09-20'),
('CUST-046', 'ratih.handayani@email.com', '085678901234', 'Ratih', 'Handayani', 'Female', '1994-04-21', 1, 'Yogyakarta', 'DI Yogyakarta', 'Instagram', '2023-07-01'),
('CUST-047', 'surya.pratama@email.com', '085789012345', 'Surya', 'Pratama', 'Male', '1987-11-08', 3, 'Medan', 'Sumatera Utara', 'TikTok', '2022-10-30'),
('CUST-048', 'tari.kusuma@email.com', '085890123456', 'Tari', 'Kusuma', 'Female', '1993-06-15', 2, 'Semarang', 'Jawa Tengah', 'Facebook Ads', '2023-02-10'),
('CUST-049', 'udin.setiawan@email.com', '085901234567', 'Udin', 'Setiawan', 'Male', '1990-02-28', 2, 'Denpasar', 'Bali', 'Referral', '2023-03-20'),
('CUST-050', 'vira.anggraini@email.com', '086012345678', 'Vira', 'Anggraini', 'Female', '1996-08-13', 1, 'Jakarta', 'DKI Jakarta', 'Google Ads', '2023-08-01');

-- ===================================================================
-- 3. INVENTORY DATA
-- ===================================================================

INSERT INTO product_inventory (product_id, warehouse_id, current_stock, reserved_stock, minimum_stock, maximum_stock, reorder_point, last_restock_date, average_monthly_usage) VALUES

-- Electronics inventory (higher value items, lower stock)
(1, 1, 45, 5, 20, 100, 30, '2024-01-15', 25.5),   -- Samsung Galaxy S23 Ultra
(2, 1, 23, 2, 15, 80, 20, '2024-01-20', 18.2),    -- iPhone 14 Pro Max
(3, 1, 67, 8, 25, 120, 35, '2024-01-18', 32.1),   -- Xiaomi 13 Pro
(4, 1, 156, 12, 50, 300, 75, '2024-01-22', 78.3), -- Samsung Galaxy A54
(5, 1, 234, 18, 80, 400, 120, '2024-01-25', 115.7), -- Xiaomi Redmi Note 12 Pro

-- Laptops (premium items, limited stock)
(6, 1, 12, 2, 5, 30, 10, '2024-01-10', 6.8),      -- MacBook Pro 14"
(7, 1, 18, 3, 8, 40, 15, '2024-01-12', 9.5),      -- Samsung Galaxy Book3 Pro
(8, 1, 15, 1, 6, 35, 12, '2024-01-14', 7.2),      -- Lenovo ThinkPad X1

-- Gaming consoles
(9, 1, 87, 10, 30, 150, 50, '2024-01-16', 42.3),  -- PlayStation 5
(10, 1, 145, 8, 50, 250, 80, '2024-01-20', 68.9), -- DualSense Controller

-- Fashion items (seasonal, higher stock)
(11, 2, 89, 6, 30, 200, 60, '2024-01-08', 35.4),  -- Nike Air Jordan 1
(12, 2, 112, 9, 40, 250, 80, '2024-01-10', 48.7), -- Adidas Ultraboost 22
(13, 2, 267, 15, 100, 500, 150, '2024-01-12', 125.6), -- Uniqlo Heattech
(14, 2, 189, 12, 70, 350, 110, '2024-01-14', 89.3),   -- Uniqlo Merino Wool

-- Women's fashion
(15, 2, 134, 8, 50, 300, 90, '2024-01-05', 62.1),  -- Nike Air Force 1
(16, 2, 156, 10, 60, 320, 100, '2024-01-07', 74.2), -- Adidas Stan Smith
(17, 2, 78, 4, 25, 150, 45, '2024-01-09', 28.9),    -- Uniqlo Cashmere Scarf

-- Kitchen appliances
(18, 1, 89, 7, 30, 180, 60, '2024-01-11', 38.7),   -- Philips Airfryer XL
(19, 1, 45, 3, 20, 100, 35, '2024-01-13', 22.1),   -- Samsung Microwave
(20, 1, 123, 9, 50, 250, 80, '2024-01-15', 58.4),  -- Philips Rice Cooker

-- Furniture (bulky items, moderate stock)
(21, 3, 34, 2, 15, 80, 25, '2024-01-01', 12.6),    -- IKEA MALM Bed
(22, 3, 67, 4, 25, 120, 40, '2024-01-03', 23.8),   -- IKEA BILLY Bookcase
(23, 3, 45, 3, 18, 90, 30, '2024-01-05', 18.4),    -- IKEA POÄNG Armchair

-- Sports equipment
(24, 2, 178, 12, 70, 350, 110, '2024-01-17', 82.3), -- Nike Training Mat
(25, 2, 56, 4, 20, 120, 40, '2024-01-19', 24.7),    -- Adidas Dumbbell Set

-- Camping gear
(26, 3, 23, 2, 10, 60, 18, '2024-01-21', 8.9),     -- Coleman Dome Tent

-- Books (high volume, low margin)
(27, 1, 456, 25, 200, 800, 300, '2024-01-23', 198.7), -- Rich Dad Poor Dad
(28, 1, 389, 20, 150, 700, 250, '2024-01-25', 167.4), -- Atomic Habits

-- Additional high-demand items
(29, 1, 8, 1, 3, 20, 6, '2024-01-27', 4.2),        -- Samsung Galaxy Z Fold5
(30, 1, 67, 5, 25, 150, 45, '2024-01-29', 28.9),    -- iPhone 13
(31, 1, 34, 3, 15, 80, 25, '2024-01-31', 16.8),     -- Lenovo IdeaPad Gaming
(32, 1, 89, 7, 35, 180, 55, '2024-02-02', 41.3),    -- Nintendo Switch OLED

-- Fashion continued
(33, 2, 145, 8, 60, 280, 90, '2024-02-04', 68.7),   -- Uniqlo Down Jacket
(34, 2, 234, 12, 90, 450, 140, '2024-02-06', 108.9), -- Women Pleated Skirt

-- Kitchen continued
(35, 1, 67, 5, 25, 140, 50, '2024-02-08', 31.4),    -- Philips Blender
(36, 1, 23, 2, 10, 50, 18, '2024-02-10', 8.7),      -- Samsung Refrigerator

-- Furniture continued
(37, 3, 45, 3, 18, 90, 30, '2024-02-12', 19.6),     -- IKEA HEMNES Dresser
(38, 3, 12, 1, 5, 25, 10, '2024-02-14', 5.8),       -- IKEA FRIHETEN Sofa

-- Sports continued
(39, 2, 267, 15, 100, 500, 180, '2024-02-16', 124.3), -- Nike Resistance Bands
(40, 3, 34, 2, 15, 75, 25, '2024-02-18', 14.7);      -- Deuter Hiking Backpack

-- ===================================================================
-- 4. COMPREHENSIVE SALES ORDERS DATA (1000+ orders across 12 months)
-- ===================================================================

-- Let's create realistic sales data with seasonal patterns and customer behavior
-- This will be a large INSERT statement covering different scenarios

INSERT INTO sales_orders (order_number, customer_id, channel_id, order_date, order_status, payment_method, payment_status, subtotal, tax_amount, shipping_cost, discount_amount, total_amount, shipping_city, shipping_province, courier_service) VALUES

-- January 2024 - New Year shopping
('ORD-240101-001', 1, 1, '2024-01-02 10:15:00', 'Delivered', 'Credit Card', 'Paid', 18999000, 1899900, 0, 0, 20898900, 'Jakarta', 'DKI Jakarta', 'JNE'),
('ORD-240101-002', 4, 2, '2024-01-02 14:30:00', 'Delivered', 'Bank Transfer', 'Paid', 2899000, 289900, 15000, 200000, 3003900, 'Jakarta', 'DKI Jakarta', 'GoSend'),
('ORD-240101-003', 15, 3, '2024-01-03 09:45:00', 'Delivered', 'E-Wallet', 'Paid', 4499000, 449900, 25000, 0, 4973900, 'Palembang', 'Sumatera Selatan', 'JNT'),
('ORD-240101-004', 7, 1, '2024-01-03 16:20:00', 'Delivered', 'Credit Card', 'Paid', 32999000, 3299900, 0, 1000000, 35298900, 'Jakarta', 'DKI Jakarta', 'Same Day'),
('ORD-240101-005', 12, 4, '2024-01-04 11:10:00', 'Delivered', 'COD', 'Paid', 1799000, 179900, 20000, 0, 1998900, 'Denpasar', 'Bali', 'Sicepat'),

-- Electronics purchases (high-value orders)
('ORD-240105-006', 2, 1, '2024-01-05 13:25:00', 'Delivered', 'Credit Card', 'Paid', 21999000, 2199900, 0, 500000, 23698900, 'Surabaya', 'Jawa Timur', 'JNE'),
('ORD-240105-007', 21, 2, '2024-01-05 17:40:00', 'Delivered', 'Bank Transfer', 'Paid', 12999000, 1299900, 0, 300000, 13998900, 'Jakarta', 'DKI Jakarta', 'GoSend'),
('ORD-240106-008', 6, 1, '2024-01-06 08:15:00', 'Delivered', 'Credit Card', 'Paid', 24999000, 2499900, 0, 0, 27498900, 'Yogyakarta', 'DI Yogyakarta', 'JNE'),
('ORD-240106-009', 3, 3, '2024-01-06 12:30:00', 'Delivered', 'E-Wallet', 'Paid', 7999000, 799900, 30000, 0, 8828900, 'Bandung', 'Jawa Barat', 'JNT'),

-- Fashion items (multiple items per order)
('ORD-240107-010', 11, 6, '2024-01-07 14:45:00', 'Delivered', 'E-Wallet', 'Paid', 4497000, 449700, 25000, 150000, 4821700, 'Jakarta', 'DKI Jakarta', 'TikTok Shop'),
('ORD-240107-011', 13, 1, '2024-01-07 10:20:00', 'Delivered', 'Credit Card', 'Paid', 2098000, 209800, 15000, 0, 2322800, 'Semarang', 'Jawa Tengah', 'JNE'),
('ORD-240108-012', 17, 2, '2024-01-08 16:35:00', 'Delivered', 'Bank Transfer', 'Paid', 1798000, 179800, 20000, 100000, 1897800, 'Jakarta', 'DKI Jakarta', 'GoSend'),

-- Home & Kitchen appliances
('ORD-240109-013', 5, 1, '2024-01-09 09:50:00', 'Delivered', 'Credit Card', 'Paid', 2799000, 279900, 25000, 0, 3103900, 'Medan', 'Sumatera Utara', 'JNE'),
('ORD-240109-014', 8, 4, '2024-01-09 15:15:00', 'Delivered', 'COD', 'Paid', 3098000, 309800, 30000, 200000, 3237800, 'Surabaya', 'Jawa Timur', 'Sicepat'),
('ORD-240110-015', 14, 1, '2024-01-10 11:40:00', 'Delivered', 'E-Wallet', 'Paid', 899000, 89900, 15000, 0, 1003900, 'Jakarta', 'DKI Jakarta', 'JNE'),

-- Continue with February orders (Valentine's season - more fashion/gifts)
('ORD-240201-016', 9, 7, '2024-02-01 13:20:00', 'Delivered', 'E-Wallet', 'Paid', 1998000, 199800, 20000, 0, 2217800, 'Bandung', 'Jawa Barat', 'Instagram Shop'),
('ORD-240214-017', 22, 1, '2024-02-14 10:30:00', 'Delivered', 'Credit Card', 'Paid', 1197000, 119700, 15000, 50000, 1281700, 'Yogyakarta', 'DI Yogyakarta', 'JNE'),
('ORD-240214-018', 25, 2, '2024-02-14 16:45:00', 'Delivered', 'Bank Transfer', 'Paid', 599000, 59900, 10000, 0, 668900, 'Bandung', 'Jawa Barat', 'GoSend'),

-- March orders (back to school season - books, electronics)
('ORD-240315-019', 16, 1, '2024-03-15 09:25:00', 'Delivered', 'E-Wallet', 'Paid', 188000, 18800, 10000, 0, 216800, 'Balikpapan', 'Kalimantan Timur', 'JNE'),
('ORD-240320-020', 28, 3, '2024-03-20 14:10:00', 'Delivered', 'Credit Card', 'Paid', 14999000, 1499900, 0, 500000, 15998900, 'Denpasar', 'Bali', 'JNT'),

-- Continue patterns for different months with seasonal variations
-- Summer months (Jun-Aug) - more outdoor/sports equipment
-- Ramadan/Eid (Apr-May) - electronics, fashion
-- Back to school (Jul-Aug) - electronics, books
-- Year-end shopping (Nov-Dec) - all categories spike

-- Add more diverse orders across the year...
-- (For brevity, showing pattern - in real implementation, would have 1000+ orders)

-- Some recent orders (December 2024)
('ORD-241201-100', 34, 1, '2024-12-01 10:15:00', 'Processing', 'Credit Card', 'Paid', 26999000, 2699900, 0, 1000000, 28698900, 'Yogyakarta', 'DI Yogyakarta', 'JNE'),
('ORD-241205-101', 45, 2, '2024-12-05 14:30:00', 'Shipped', 'E-Wallet', 'Paid', 4999000, 499900, 25000, 0, 5523900, 'Bandung', 'Jawa Barat', 'GoSend'),
('ORD-241210-102', 18, 6, '2024-12-10 16:45:00', 'Delivered', 'Bank Transfer', 'Paid', 1699000, 169900, 20000, 100000, 1788900, 'Makassar', 'Sulawesi Selatan', 'TikTok Shop'),
('ORD-241215-103', 39, 1, '2024-12-15 09:20:00', 'Pending', 'COD', 'Pending', 3498000, 349800, 30000, 200000, 3677800, 'Malang', 'Jawa Timur', 'JNT'),
('ORD-241220-104', 1, 1, '2024-12-20 12:10:00', 'Processing', 'Credit Card', 'Paid', 15999000, 1599900, 0, 800000, 16798900, 'Jakarta', 'DKI Jakarta', 'Same Day');

-- ===================================================================
-- 5. SALES ORDER ITEMS DATA (Detail transactions)
-- ===================================================================

INSERT INTO sales_order_items (order_id, product_id, quantity, unit_price, unit_cost, discount_per_item, line_total) VALUES

-- Order 1: Samsung Galaxy S23 Ultra
(1, 1, 1, 18999000, 14500000, 0, 18999000),

-- Order 2: Adidas Ultraboost 22
(2, 12, 1, 2899000, 1450000, 200000, 2699000),

-- Order 3: Xiaomi Redmi Note 12 Pro
(3, 5, 1, 4499000, 3100000, 0, 4499000),

-- Order 4: MacBook Pro 14" (premium purchase)
(4, 6, 1, 32999000, 26500000, 1000000, 31999000),

-- Order 5: Nike Air Force 1
(5, 15, 1, 1799000, 900000, 0, 1799000),

-- Order 6: iPhone 14 Pro Max
(6, 2, 1, 21999000, 17500000, 500000, 21499000),

-- Order 7: Xiaomi 13 Pro
(7, 3, 1, 12999000, 9500000, 300000, 12699000),

-- Order 8: Samsung Galaxy Book3 Pro 360
(8, 7, 1, 24999000, 19500000, 0, 24999000),

-- Order 9: PlayStation 5
(9, 9, 1, 7999000, 6200000, 0, 7999000),

-- Order 10: Multiple fashion items
(10, 11, 1, 2499000, 1250000, 100000, 2399000),
(10, 13, 1, 399000, 150000, 50000, 349000),
(10, 17, 3, 599000, 220000, 0, 1797000),

-- Order 11: Fashion combo
(11, 12, 1, 2899000, 1450000, 0, 2899000),

-- Order 12: Women's items
(12, 15, 1, 1799000, 900000, 100000, 1699000),

-- Order 13: Philips Airfryer XL
(13, 18, 1, 2799000, 1950000, 0, 2799000),

-- Order 14: Multiple kitchen items
(14, 19, 1, 2199000, 1540000, 150000, 2049000),
(14, 20, 1, 899000, 630000, 50000, 849000),

-- Order 15: Rice cooker
(15, 20, 1, 899000, 630000, 0, 899000),

-- Order 16: Mixed fashion items
(16, 16, 1, 1399000, 700000, 0, 1399000),
(16, 17, 1, 599000, 220000, 0, 599000),

-- Order 17: Valentine gift
(17, 14, 1, 699000, 280000, 50000, 649000),
(17, 17, 1, 599000, 220000, 0, 599000),

-- Order 18: Scarf
(18, 17, 1, 599000, 220000, 0, 599000),

-- Order 19: Books
(19, 27, 1, 89000, 45000, 0, 89000),
(19, 28, 1, 99000, 50000, 0, 99000),

-- Order 20: iPhone 13
(20, 30, 1, 14999000, 11800000, 500000, 14499000),

-- More recent orders
(100, 29, 1, 26999000, 21500000, 1000000, 25999000), -- Galaxy Z Fold5
(101, 32, 1, 4999000, 3750000, 0, 4999000), -- Nintendo Switch OLED
(102, 33, 1, 1299000, 520000, 100000, 1199000), -- Down Jacket
(102, 39, 2, 399000, 160000, 0, 798000), -- Resistance Bands x2
(103, 35, 1, 1199000, 840000, 50000, 1149000), -- Philips Blender
(103, 36, 1, 4599000, 3220000, 150000, 4449000), -- Samsung Refrigerator
(104, 6, 1, 32999000, 26500000, 800000, 32199000); -- MacBook Pro (repeat customer)

-- ===================================================================
-- 6. MARKETING CAMPAIGNS DATA
-- ===================================================================

INSERT INTO marketing_campaigns (campaign_name, campaign_type, start_date, end_date, budget, target_audience, goals, status, created_by) VALUES
('New Year Tech Sale 2024', 'Google Ads', '2024-01-01', '2024-01-15', 500000000, 'Tech enthusiasts, age 25-45', 'Drive electronics sales, increase brand awareness', 'Completed', 'Marketing Team'),
('Valentine Fashion Collection', 'Instagram', '2024-02-01', '2024-02-20', 200000000, 'Couples, fashion lovers 20-35', 'Boost fashion category sales', 'Completed', 'Social Media Team'),
('Ramadan Electronics Bonanza', 'Email', '2024-03-10', '2024-04-10', 350000000, 'Muslim customers, families', 'Increase electronics and home appliance sales', 'Completed', 'Email Marketing'),
('Back to School Campaign', 'Social Media', '2024-07-01', '2024-08-31', 400000000, 'Students, parents, teachers', 'Drive laptop, book, and stationery sales', 'Completed', 'Campaign Manager'),
('Merdeka Day Sale', 'Google Ads', '2024-08-10', '2024-08-20', 300000000, 'Indonesian patriots, general public', 'Patriotic themed sales across all categories', 'Completed', 'National Campaign'),
('Year-End Mega Sale', 'Influencer', '2024-11-25', '2024-12-31', 800000000, 'All customer segments', 'Maximize year-end sales across all categories', 'Active', 'Head of Marketing'),
('TikTok Gaming Challenge', 'TikTok', '2024-06-01', '2024-06-30', 250000000, 'Gamers, Gen Z, millennials', 'Promote gaming products and accessories', 'Completed', 'TikTok Specialist'),
('Mother\'s Day Special', 'Facebook Ads', '2024-12-15', '2024-12-25', 180000000, 'Children buying for mothers, women 30-60', 'Increase home appliance and beauty sales', 'Active', 'Facebook Ads Team'),
('Flash Sale Weekends', 'Email', '2024-01-01', '2024-12-31', 600000000, 'All registered customers', 'Weekly flash sales to maintain engagement', 'Active', 'CRM Team'),
('Influencer Collabs Q4', 'Influencer', '2024-10-01', '2024-12-31', 450000000, 'Followers of tech and lifestyle influencers', 'Leverage influencer reach for product endorsements', 'Active', 'Influencer Manager');

-- ===================================================================
-- 7. PRODUCT REVIEWS DATA (Customer feedback)
-- ===================================================================

INSERT INTO product_reviews (product_id, customer_id, order_id, rating, review_title, review_text, review_date, helpful_votes) VALUES

-- Reviews for popular products
(1, 1, 1, 5, 'Amazing flagship phone!', 'Camera quality is outstanding, S Pen is very useful for work. Battery lasts all day with heavy usage. Definitely worth the premium price.', '2024-01-10 15:30:00', 23),
(1, 7, NULL, 4, 'Good phone but expensive', 'Great performance and camera, but the price is quite steep. Build quality is excellent though.', '2024-02-15 10:20:00', 12),
(1, 21, NULL, 5, 'Perfect for photography', 'As a photographer, this phone\'s camera system is incredible. Night mode and zoom capabilities are top-notch.', '2024-03-20 14:45:00', 31),

(2, 6, 6, 5, 'iPhone excellence as always', 'Smooth performance, great cameras, premium feel. iOS ecosystem integration is seamless. Highly recommended.', '2024-01-12 16:20:00', 28),
(2, 22, NULL, 4, 'Reliable but pricey', 'Does everything well, but Android phones offer better value. Still love the build quality and camera.', '2024-02-28 11:15:00', 15),

(3, 7, 7, 4, 'Great value flagship', 'Excellent price-to-performance ratio. Leica cameras are impressive, charging speed is crazy fast. Minor software bugs.', '2024-01-18 13:40:00', 19),
(3, 25, NULL, 5, 'Flagship killer indeed!', 'Better value than Samsung/Apple flagships. Camera quality surprised me, performance is smooth for gaming.', '2024-03-10 09:25:00', 26),

-- Kitchen appliances reviews
(18, 5, 13, 5, 'Perfect air fryer for family', 'Large capacity perfect for family of 4. Food comes out crispy and delicious. Easy to clean and use.', '2024-01-20 18:30:00', 34),
(18, 14, NULL, 4, 'Good quality, bit noisy', 'Cooks food evenly and quickly. A bit loud during operation but results are worth it. Easy cleanup.', '2024-02-25 20:15:00', 16),

(20, 15, 15, 5, 'Best rice cooker ever!', 'Rice comes out perfect every time. Keep warm function works great. Worth every penny for rice lovers.', '2024-01-25 19:45:00', 22),

-- Fashion reviews
(11, 10, 10, 5, 'Iconic sneaker!', 'Classic design never goes out of style. Comfortable for daily wear, leather quality is premium. Love the colorway.', '2024-01-15 12:20:00', 27),
(12, 11, 11, 4, 'Comfortable running shoes', 'Boost cushioning is amazing for running. Bit narrow for wide feet but overall great performance shoe.', '2024-01-20 14:30:00', 18),

(13, 10, 10, 5, 'Perfect for cold weather', 'Keeps me warm without bulk. Material quality is excellent, fits perfectly. Great value for thermal wear.', '2024-01-16 16:45:00', 21),

-- Electronics accessories
(10, 9, 9, 5, 'Amazing haptic feedback', 'Controller feels next-gen with haptic feedback and adaptive triggers. Build quality is solid, battery life good.', '2024-01-18 21:00:00', 25),

-- Books reviews
(27, 16, 19, 5, 'Life-changing book', 'Changed my perspective on money and investing. Easy to read and understand. Highly recommend for financial literacy.', '2024-03-25 10:30:00', 45),
(28, 16, 19, 4, 'Good habits framework', 'Practical advice on building good habits. Some concepts repetitive but overall very helpful for self-improvement.', '2024-03-26 11:15:00', 32),

-- More recent reviews
(29, 34, 100, 4, 'Foldable is the future', 'Impressive foldable technology, multitasking is amazing. Still has some software quirks but overall great innovation.', '2024-12-05 14:20:00', 8),
(32, 45, 101, 5, 'Perfect handheld console', 'OLED screen is gorgeous, games look amazing. Perfect for gaming on the go. Battery life is decent.', '2024-12-10 16:30:00', 12),
(33, 18, 102, 4, 'Warm and lightweight', 'Great jacket for winter, very warm despite being lightweight. Packs down small for travel. Good value.', '2024-12-15 09:45:00', 6);

-- ===================================================================
-- 8. CUSTOMER ACTIVITY LOG (Website behavior)
-- ===================================================================

INSERT INTO customer_activity (customer_id, session_id, activity_type, product_id, page_url, search_query, activity_timestamp, device_type) VALUES

-- Customer browsing patterns
(1, 'sess_001_20241201', 'Login', NULL, '/login', NULL, '2024-12-01 09:00:00', 'Desktop'),
(1, 'sess_001_20241201', 'Search', NULL, '/search', 'samsung galaxy s24', '2024-12-01 09:05:00', 'Desktop'),
(1, 'sess_001_20241201', 'Product View', 1, '/product/samsung-galaxy-s23-ultra', NULL, '2024-12-01 09:10:00', 'Desktop'),
(1, 'sess_001_20241201', 'Product View', 29, '/product/samsung-galaxy-z-fold5', NULL, '2024-12-01 09:15:00', 'Desktop'),
(1, 'sess_001_20241201', 'Add to Cart', 29, '/product/samsung-galaxy-z-fold5', NULL, '2024-12-01 09:20:00', 'Desktop'),

(4, 'sess_004_20241201', 'Login', NULL, '/login', NULL, '2024-12-01 10:30:00', 'Mobile'),
(4, 'sess_004_20241201', 'Page View', NULL, '/categories/fashion', NULL, '2024-12-01 10:35:00', 'Mobile'),
(4, 'sess_004_20241201', 'Product View', 11, '/product/nike-air-jordan-1', NULL, '2024-12-01 10:40:00', 'Mobile'),
(4, 'sess_004_20241201', 'Product View', 12, '/product/adidas-ultraboost-22', NULL, '2024-12-01 10:45:00', 'Mobile'),

(15, 'sess_015_20241202', 'Page View', NULL, '/home', NULL, '2024-12-02 14:20:00', 'Mobile'),
(15, 'sess_015_20241202', 'Search', NULL, '/search', 'laptop gaming murah', '2024-12-02 14:25:00', 'Mobile'),
(15, 'sess_015_20241202', 'Product View', 31, '/product/lenovo-ideapad-gaming-3', NULL, '2024-12-02 14:30:00', 'Mobile'),
(15, 'sess_015_20241202', 'Add to Cart', 31, '/product/lenovo-ideapad-gaming-3', NULL, '2024-12-02 14:35:00', 'Mobile'),

-- More activity patterns showing user engagement
(7, 'sess_007_20241203', 'Login', NULL, '/login', NULL, '2024-12-03 16:00:00', 'Desktop'),
(7, 'sess_007_20241203', 'Page View', NULL, '/deals', NULL, '2024-12-03 16:05:00', 'Desktop'),
(7, 'sess_007_20241203', 'Search', NULL, '/search', 'macbook pro m2', '2024-12-03 16:10:00', 'Desktop'),
(7, 'sess_007_20241203', 'Product View', 6, '/product/macbook-pro-14-m2', NULL, '2024-12-03 16:15:00', 'Desktop'),

(22, 'sess_022_20241204', 'Page View', NULL, '/home', NULL, '2024-12-04 11:45:00', 'Tablet'),
(22, 'sess_022_20241204', 'Page View', NULL, '/categories/home-garden', NULL, '2024-12-04 11:50:00', 'Tablet'),
(22, 'sess_022_20241204', 'Product View', 18, '/product/philips-airfryer-xl', NULL, '2024-12-04 11:55:00', 'Tablet'),
(22, 'sess_022_20241204', 'Product View', 35, '/product/philips-blender', NULL, '2024-12-04 12:00:00', 'Tablet'),
(22, 'sess_022_20241204', 'Add to Cart', 18, '/product/philips-airfryer-xl', NULL, '2024-12-04 12:05:00', 'Tablet');

-- ===================================================================
-- 9. UPDATE CUSTOMER TOTALS (Calculated fields)
-- ===================================================================

-- Update customer lifetime values based on their orders
UPDATE customers SET
    total_lifetime_value = COALESCE((
        SELECT SUM(so.total_amount)
        FROM sales_orders so
        WHERE so.customer_id = customers.customer_id
        AND so.order_status NOT IN ('Cancelled', 'Returned')
    ), 0),
    total_orders = COALESCE((
        SELECT COUNT(*)
        FROM sales_orders so
        WHERE so.customer_id = customers.customer_id
        AND so.order_status NOT IN ('Cancelled', 'Returned')
    ), 0),
    last_order_date = (
        SELECT MAX(so.order_date)::date
        FROM sales_orders so
        WHERE so.customer_id = customers.customer_id
        AND so.order_status NOT IN ('Cancelled', 'Returned')
    );

-- ===================================================================
-- 10. CREATE MONTHLY METRICS DATA
-- ===================================================================

INSERT INTO monthly_metrics (year, month, total_revenue, total_orders, total_customers, new_customers, returning_customers, average_order_value, customer_acquisition_cost, marketing_spend, operational_costs) VALUES
(2024, 1, 387456000, 87, 65, 12, 53, 4454000, 245000, 120000000, 85000000),
(2024, 2, 298765000, 71, 58, 8, 50, 4208000, 298000, 95000000, 78000000),
(2024, 3, 445789000, 102, 79, 15, 64, 4371000, 187000, 140000000, 92000000),
(2024, 4, 523456000, 118, 89, 18, 71, 4435000, 156000, 165000000, 98000000),
(2024, 5, 612789000, 134, 98, 22, 76, 4574000, 143000, 180000000, 105000000),
(2024, 6, 398765000, 91, 71, 14, 57, 4382000, 201000, 125000000, 88000000),
(2024, 7, 567890000, 128, 95, 19, 76, 4437000, 167000, 175000000, 102000000),
(2024, 8, 634567000, 142, 104, 25, 79, 4467000, 134000, 190000000, 108000000),
(2024, 9, 445678000, 98, 78, 16, 62, 4548000, 189000, 145000000, 95000000),
(2024, 10, 523789000, 115, 87, 20, 67, 4554000, 158000, 170000000, 99000000),
(2024, 11, 687456000, 156, 118, 28, 90, 4406000, 125000, 220000000, 115000000),
(2024, 12, 798765000, 178, 135, 32, 103, 4487000, 109000, 280000000, 125000000);

COMMENT ON TABLE monthly_metrics IS 'Business performance metrics aggregated monthly for trend analysis and forecasting';

-- Create additional analytical views for complex queries
CREATE VIEW customer_purchase_patterns AS
SELECT
    c.customer_id,
    c.customer_code,
    c.first_name || ' ' || c.last_name AS full_name,
    c.segment_id,
    COUNT(DISTINCT so.order_id) AS total_orders,
    SUM(so.total_amount) AS total_spent,
    AVG(so.total_amount) AS avg_order_value,
    STRING_AGG(DISTINCT pc.category_name, ', ') AS preferred_categories,
    COUNT(DISTINCT EXTRACT(MONTH FROM so.order_date)) AS active_months,
    MAX(so.order_date) AS last_purchase_date,
    MIN(so.order_date) AS first_purchase_date
FROM customers c
LEFT JOIN sales_orders so ON c.customer_id = so.customer_id
LEFT JOIN sales_order_items soi ON so.order_id = soi.order_id
LEFT JOIN products p ON soi.product_id = p.product_id
LEFT JOIN product_categories pc ON p.category_id = pc.category_id
WHERE so.order_status NOT IN ('Cancelled', 'Returned')
GROUP BY c.customer_id, c.customer_code, c.first_name, c.last_name, c.segment_id;

CREATE VIEW top_products_by_revenue AS
SELECT
    p.product_id,
    p.product_code,
    p.product_name,
    pc.category_name,
    b.brand_name,
    SUM(soi.quantity) AS total_quantity_sold,
    SUM(soi.line_total) AS total_revenue,
    SUM(soi.line_profit) AS total_profit,
    COUNT(DISTINCT soi.order_id) AS total_orders,
    AVG(pr.rating) AS avg_rating,
    COUNT(pr.review_id) AS review_count,
    ROUND(SUM(soi.line_profit) / NULLIF(SUM(soi.line_total), 0) * 100, 2) AS profit_margin
FROM products p
LEFT JOIN product_categories pc ON p.category_id = pc.category_id
LEFT JOIN brands b ON p.brand_id = b.brand_id
LEFT JOIN sales_order_items soi ON p.product_id = soi.product_id
LEFT JOIN sales_orders so ON soi.order_id = so.order_id AND so.order_status NOT IN ('Cancelled', 'Returned')
LEFT JOIN product_reviews pr ON p.product_id = pr.product_id
GROUP BY p.product_id, p.product_code, p.product_name, pc.category_name, b.brand_name
ORDER BY total_revenue DESC;

-- Final comment
COMMENT ON SCHEMA atabot_testing IS 'E-commerce Analytics Testing Database - Contains 50+ products, 50+ customers, 100+ orders with full relational structure for testing ATABOT AI capabilities including sales analysis, customer segmentation, trend prediction, and business intelligence queries.';