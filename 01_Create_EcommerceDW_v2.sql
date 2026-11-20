/* =====================================================================
   DỰ ÁN: KHO DỮ LIỆU THƯƠNG MẠI ĐIỆN TỬ (ECOMMERCE DATA WAREHOUSE)
   FILE: 01_Create_EcommerceDW_v2.sql
   MÔ HÌNH: KIMBALL GALAXY SCHEMA MỞ RỘNG (9 DIMENSIONS + 7 FACTS)
   ĐẶC TÍNH NỔI BẬT:
     1. Multi-Currency Support (Hỗ trợ 7 loại ngoại tệ và quy đổi chuẩn USD)
     2. SCD Type 2 trên Dim_Customer & SCD Type 1 trên Dim_Product
     3. Junk Dimension (Dim_Order_Profile) & Extended Dim_Region
     4. Hệ thống Bảng Điều phối, Kiểm toán & Cách ly lỗi (etl_batch, etl_reject, ETL_Watermark)
     5. Tối ưu hóa chỉ mục (Indexes) phục vụ SSAS Cube Processing & MDX
   NỀN TẢNG: Microsoft SQL Server 2016+
   ===================================================================== */

USE master;
GO

-- 1. KHỞI TẠO CƠ SỞ DỮ LIỆU
IF DB_ID(N'EcommerceDW') IS NULL
BEGIN
    CREATE DATABASE EcommerceDW;
    PRINT N'Đã tạo mới Database EcommerceDW.';
END
ELSE
BEGIN
    PRINT N'Database EcommerceDW đã tồn tại, tiếp tục áp dụng cấu trúc mới.';
END
GO

ALTER DATABASE EcommerceDW SET RECOVERY SIMPLE;
GO

USE EcommerceDW;
GO

-- =====================================================================
-- 2. HỆ THỐNG BẢNG ĐIỀU PHỐI, QUARANTINE & AUDIT LOGGING (METADATA DDL)
-- =====================================================================

-- 2.1 Bảng Nhật ký Phiên chạy ETL (etl_batch)
IF OBJECT_ID('dbo.etl_batch', 'U') IS NOT NULL DROP TABLE dbo.etl_batch;
GO

CREATE TABLE dbo.etl_batch (
    batch_id         INT IDENTITY(1,1) NOT NULL,
    load_from        DATE              NOT NULL,
    load_to          DATE              NOT NULL,
    started_at       DATETIME2         NOT NULL DEFAULT SYSDATETIME(),
    finished_at      DATETIME2         NULL,
    status           VARCHAR(20)       NOT NULL DEFAULT 'RUNNING', -- RUNNING, SUCCESS, FAILED
    rows_before      INT               NULL,
    rows_after       INT               NULL,
    rows_rejected    INT               NULL,
    execution_host   NVARCHAR(100)     NOT NULL DEFAULT HOST_NAME(),
    notes            NVARCHAR(500)     NULL,
    CONSTRAINT PK_etl_batch PRIMARY KEY CLUSTERED (batch_id),
    CONSTRAINT CK_etl_batch_status CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED'))
);
GO

-- 2.2 Bảng Cách ly & Phân loại Lỗi (etl_reject)
IF OBJECT_ID('dbo.etl_reject', 'U') IS NOT NULL DROP TABLE dbo.etl_reject;
GO

CREATE TABLE dbo.etl_reject (
    reject_id        BIGINT IDENTITY(1,1) NOT NULL,
    batch_id         INT                  NULL,
    source_table     VARCHAR(100)         NOT NULL,
    natural_key      VARCHAR(200)         NULL, -- VD: order_id hoặc customer_id
    reject_category  VARCHAR(50)          NOT NULL, -- E01_NULL, E02_TYPE, E03_BUSINESS, E04_ORPHAN, E05_DUP, E06_RANGE
    reject_reason    NVARCHAR(500)        NULL,
    raw_data_snippet NVARCHAR(MAX)        NULL,
    logged_at        DATETIME2            NOT NULL DEFAULT SYSDATETIME(),
    reprocessed      BIT                  NOT NULL DEFAULT 0,
    reprocess_notes  NVARCHAR(300)        NULL,
    CONSTRAINT PK_etl_reject PRIMARY KEY CLUSTERED (reject_id),
    CONSTRAINT FK_etl_reject_batch FOREIGN KEY (batch_id) REFERENCES dbo.etl_batch(batch_id)
);
GO

-- 2.3 Bảng Mốc nạp tăng dần (ETL_Watermark)
IF OBJECT_ID('dbo.ETL_Watermark', 'U') IS NOT NULL DROP TABLE dbo.ETL_Watermark;
GO

CREATE TABLE dbo.ETL_Watermark (
    table_name       VARCHAR(100) NOT NULL,
    last_loaded_key  VARCHAR(50)  NULL,
    last_run_time    DATETIME2    NOT NULL DEFAULT '2020-01-01',
    total_rows_last  INT          NULL,
    CONSTRAINT PK_ETL_Watermark PRIMARY KEY CLUSTERED (table_name)
);
GO

-- 2.4 Bảng Cửa sổ nạp hiện hành (etl_load_window)
IF OBJECT_ID('dbo.etl_load_window', 'U') IS NOT NULL DROP TABLE dbo.etl_load_window;
GO

CREATE TABLE dbo.etl_load_window (
    window_id  INT IDENTITY(1,1) PRIMARY KEY,
    load_from  DATE      NOT NULL,
    load_to    DATE      NOT NULL,
    created_at DATETIME2 NOT NULL DEFAULT SYSDATETIME()
);
GO


-- =====================================================================
-- 3. XÓA CÁC BẢNG FACT VÀ DIM THEO THỨ TỰ PHỤ THUỘC (NẾU CÓ)
-- =====================================================================

-- Xóa Facts
IF OBJECT_ID('dbo.Fact_Sales', 'U') IS NOT NULL DROP TABLE dbo.Fact_Sales;
IF OBJECT_ID('dbo.Fact_Order_Fulfillment', 'U') IS NOT NULL DROP TABLE dbo.Fact_Order_Fulfillment;
IF OBJECT_ID('dbo.Fact_Return', 'U') IS NOT NULL DROP TABLE dbo.Fact_Return;
IF OBJECT_ID('dbo.Fact_Customer_Feedback', 'U') IS NOT NULL DROP TABLE dbo.Fact_Customer_Feedback;
IF OBJECT_ID('dbo.Fact_Customer_Acquisition', 'U') IS NOT NULL DROP TABLE dbo.Fact_Customer_Acquisition;
IF OBJECT_ID('dbo.Fact_Customer_Snapshot', 'U') IS NOT NULL DROP TABLE dbo.Fact_Customer_Snapshot;
IF OBJECT_ID('dbo.Fact_Campaign_Promotion', 'U') IS NOT NULL DROP TABLE dbo.Fact_Campaign_Promotion;
GO

-- Xóa Dimensions
IF OBJECT_ID('dbo.Dim_Customer', 'U') IS NOT NULL DROP TABLE dbo.Dim_Customer;
IF OBJECT_ID('dbo.Dim_Product', 'U') IS NOT NULL DROP TABLE dbo.Dim_Product;
IF OBJECT_ID('dbo.Dim_Date', 'U') IS NOT NULL DROP TABLE dbo.Dim_Date;
IF OBJECT_ID('dbo.Dim_Currency', 'U') IS NOT NULL DROP TABLE dbo.Dim_Currency;
IF OBJECT_ID('dbo.Dim_Region', 'U') IS NOT NULL DROP TABLE dbo.Dim_Region;
IF OBJECT_ID('dbo.Dim_Shipping', 'U') IS NOT NULL DROP TABLE dbo.Dim_Shipping;
IF OBJECT_ID('dbo.Dim_Order_Profile', 'U') IS NOT NULL DROP TABLE dbo.Dim_Order_Profile;
IF OBJECT_ID('dbo.Dim_Marketing_Campaign', 'U') IS NOT NULL DROP TABLE dbo.Dim_Marketing_Campaign;
IF OBJECT_ID('dbo.Dim_Return_Profile', 'U') IS NOT NULL DROP TABLE dbo.Dim_Return_Profile;
GO


-- =====================================================================
-- 4. TẠO 9 BẢNG CHIỀU (DIMENSIONS)
-- =====================================================================

-- 4.1 Dim_Date (Smart Key: YYYYMMDD, SCD Type 0, Role-Playing)
CREATE TABLE dbo.Dim_Date (
    date_key        INT          NOT NULL,            -- YYYYMMDD
    full_date       DATE         NOT NULL,
    [year]          SMALLINT     NOT NULL,
    [quarter]       TINYINT      NOT NULL,
    quarter_name    VARCHAR(10)  NOT NULL,            -- Q1, Q2, Q3, Q4
    [month]         TINYINT      NOT NULL,
    month_name      VARCHAR(20)  NOT NULL,            -- January, February...
    [day]           TINYINT      NOT NULL,
    day_of_week     VARCHAR(20)  NOT NULL,            -- Monday, Tuesday...
    day_of_week_num TINYINT      NOT NULL,            -- 1 (Mon) -> 7 (Sun)
    is_weekend      BIT          NOT NULL,
    week_of_year    TINYINT      NOT NULL,
    year_month      CHAR(7)      NOT NULL,            -- YYYY-MM
    season          VARCHAR(20)  NOT NULL,            -- Spring, Summer, Autumn, Winter
    is_month_end    BIT          NOT NULL,            -- Phục vụ nạp Snapshot cuối tháng
    CONSTRAINT PK_Dim_Date PRIMARY KEY CLUSTERED (date_key),
    CONSTRAINT UQ_Dim_Date_full_date UNIQUE (full_date)
);
GO

-- 4.2 Dim_Currency (CHIỀU NGOẠI TỆ & QUY ĐỔI CHUẨN TÀI CHÍNH QUỐC TẾ)
CREATE TABLE dbo.Dim_Currency (
    currency_key         INT IDENTITY(1,1) NOT NULL,
    currency_code        CHAR(3)           NOT NULL,   -- USD, EUR, GBP, CAD, AUD, INR, AED
    currency_name        NVARCHAR(50)      NOT NULL,
    country              NVARCHAR(100)     NULL,
    exchange_rate_to_usd DECIMAL(18,6)     NOT NULL DEFAULT 1.0, -- Tỷ giá quy đổi sang USD
    symbol               NVARCHAR(10)      NULL,
    is_base_currency     BIT               NOT NULL DEFAULT 0,   -- 1 cho USD
    CONSTRAINT PK_Dim_Currency PRIMARY KEY CLUSTERED (currency_key),
    CONSTRAINT UQ_Dim_Currency_code UNIQUE (currency_code)
);
GO

-- 4.3 Dim_Product (Denormalized Hierarchy: Category -> Subcategory -> Brand -> Product)
CREATE TABLE dbo.Dim_Product (
    product_key        INT IDENTITY(1,1) NOT NULL,
    product_id         VARCHAR(50)       NOT NULL,    -- Natural Business Key
    product_name       NVARCHAR(255)     NULL,
    brand              NVARCHAR(100)     NULL,
    subcategory        NVARCHAR(100)     NULL,
    category           NVARCHAR(100)     NULL,
    supplier           NVARCHAR(150)     NULL,
    unit_price         DECIMAL(18,4)     NULL,
    product_cost       DECIMAL(18,4)     NULL,
    price_tier         VARCHAR(30)       NULL,        -- Budget, Mid-Range, Premium, Luxury
    product_rating     DECIMAL(5,2)      NULL,
    rating_group       VARCHAR(30)       NULL,        -- Low (<3.0), Average (3-4), High (>4.0)
    CONSTRAINT PK_Dim_Product PRIMARY KEY CLUSTERED (product_key),
    CONSTRAINT UQ_Dim_Product_id UNIQUE (product_id)
);
GO

-- 4.4 Dim_Customer (SCD Type 2: Theo dõi biến động Segment & Địa chỉ theo thời gian)
CREATE TABLE dbo.Dim_Customer (
    customer_key              INT IDENTITY(1,1) NOT NULL,
    customer_id               VARCHAR(50)       NOT NULL,   -- Natural Business Key
    customer_name             NVARCHAR(150)     NULL,
    customer_age              SMALLINT          NULL,
    age_group                 VARCHAR(30)       NULL,       -- 18-25, 26-35, 36-50, 50+
    gender                    VARCHAR(20)       NULL,
    customer_segment          VARCHAR(50)       NULL,       -- Consumer, Corporate, Home Office, Small Business, Premium
    customer_type             VARCHAR(50)       NULL,       -- Loyal, Returning, New
    customer_city             NVARCHAR(100)     NULL,
    customer_state            NVARCHAR(100)     NULL,
    customer_country          NVARCHAR(100)     NULL,
    region                    VARCHAR(50)       NULL,
    customer_postal_code      VARCHAR(30)       NULL,
    customer_acquisition_cost DECIMAL(18,4)     NULL,
    is_current                BIT               NOT NULL DEFAULT 1,
    effective_start           DATE              NOT NULL DEFAULT '2020-01-01',
    effective_end             DATE              NULL,
    CONSTRAINT PK_Dim_Customer PRIMARY KEY CLUSTERED (customer_key)
);
GO

-- 4.5 Dim_Region (Mở rộng: Phân cấp Châu lục, Múi giờ và Đồng tiền mặc định)
CREATE TABLE dbo.Dim_Region (
    region_key            INT IDENTITY(1,1) NOT NULL,
    region                VARCHAR(50)       NULL,
    [state]               NVARCHAR(100)     NULL,
    country               NVARCHAR(100)     NULL,
    continent             VARCHAR(50)       NULL,       -- North America, Europe, Asia, Oceania, Middle East
    timezone              VARCHAR(50)       NULL,
    default_currency_code CHAR(3)           NULL,       -- USD, EUR, GBP, AUD, CAD, INR, AED
    CONSTRAINT PK_Dim_Region PRIMARY KEY CLUSTERED (region_key),
    CONSTRAINT UQ_Dim_Region UNIQUE (region, [state], country)
);
GO

-- 4.6 Dim_Shipping (Vận hành giao hàng & Kho bãi)
CREATE TABLE dbo.Dim_Shipping (
    shipping_key            INT IDENTITY(1,1) NOT NULL,
    shipping_method         NVARCHAR(100)     NULL,        -- Standard, Express, Economy, Same Day
    warehouse               NVARCHAR(100)     NULL,        -- WH-001 -> WH-019
    shipping_speed_category VARCHAR(50)       NULL,        -- Fast, Standard, Economy
    CONSTRAINT PK_Dim_Shipping PRIMARY KEY CLUSTERED (shipping_key),
    CONSTRAINT UQ_Dim_Shipping UNIQUE (shipping_method, warehouse)
);
GO

-- 4.7 Dim_Order_Profile (JUNK DIMENSION - Thu gọn 5 cờ trạng thái giao dịch)
CREATE TABLE dbo.Dim_Order_Profile (
    order_profile_key INT IDENTITY(1,1) NOT NULL,
    order_status      VARCHAR(50)       NULL,   -- Completed, Cancelled, Pending, Processing
    payment_method    VARCHAR(50)       NULL,   -- Credit Card, Digital Wallet, Debit Card, Cash on Delivery
    payment_status    VARCHAR(50)       NULL,   -- Paid, Pending, Failed, Refunded
    sales_channel     VARCHAR(50)       NULL,   -- Website, Mobile App, Store POS, Social Media
    delivery_status   VARCHAR(50)       NULL,   -- On Time, Delayed, Cancelled
    CONSTRAINT PK_Dim_Order_Profile PRIMARY KEY CLUSTERED (order_profile_key),
    CONSTRAINT UQ_Dim_Order_Profile UNIQUE (order_status, payment_method, payment_status, sales_channel, delivery_status)
);
GO

-- 4.8 Dim_Marketing_Campaign (Chiều Tiếp thị & Khuyến mãi)
CREATE TABLE dbo.Dim_Marketing_Campaign (
    marketing_key     INT IDENTITY(1,1) NOT NULL,
    marketing_channel VARCHAR(100)      NULL,   -- Google Ads, Facebook, Direct, Email, YouTube...
    campaign_name     VARCHAR(150)      NULL,   -- Summer_Sale, Black_Friday, Default_Campaign...
    coupon_code       VARCHAR(50)       NULL,   -- Mã giảm giá hoặc 'NO_COUPON'
    has_coupon_flag   BIT               NOT NULL DEFAULT 0,
    channel_type      VARCHAR(50)       NULL,   -- Paid Media, Owned Media, Organic
    CONSTRAINT PK_Dim_Marketing_Campaign PRIMARY KEY CLUSTERED (marketing_key),
    CONSTRAINT UQ_Dim_Marketing_Campaign UNIQUE (marketing_channel, campaign_name, coupon_code)
);
GO

-- 4.9 Dim_Return_Profile (Chiều Hồ sơ Trả hàng)
CREATE TABLE dbo.Dim_Return_Profile (
    return_profile_key     INT IDENTITY(1,1) NOT NULL,
    return_status          VARCHAR(50)       NULL,   -- Returned, Cancelled, Not Returned
    return_reason          NVARCHAR(150)     NULL,   -- Defective Product, Wrong Item, Late Delivery...
    return_reason_category VARCHAR(50)       NULL,   -- Product Quality, Logistics, Customer Choice
    CONSTRAINT PK_Dim_Return_Profile PRIMARY KEY CLUSTERED (return_profile_key),
    CONSTRAINT UQ_Dim_Return_Profile UNIQUE (return_status, return_reason)
);
GO


-- =====================================================================
-- 5. TẠO 7 BẢNG SỰ KIỆN (FACT TABLES - CHUẨN ĐA TIỀN TỆ & KIMBALL)
-- =====================================================================

-- 5.1 Fact_Sales (Core Transaction Fact - Grain: 1 mặt hàng trong 1 đơn)
CREATE TABLE dbo.Fact_Sales (
    order_id                VARCHAR(50)   NOT NULL,   -- Degenerate Dimension
    product_key             INT           NOT NULL,
    date_key                INT           NOT NULL,   -- Ngày đặt hàng
    customer_key            INT           NOT NULL,
    region_key              INT           NULL,
    order_profile_key       INT           NULL,
    marketing_key           INT           NULL,
    currency_key            INT           NOT NULL,   -- Khóa liên kết ngoại tệ
    quantity                INT           NOT NULL,
    -- Số liệu nguyên tệ gốc:
    gross_sales             DECIMAL(18,4) NOT NULL,
    discount_amount         DECIMAL(18,4) NOT NULL,
    tax_amount              DECIMAL(18,4) NOT NULL,
    net_revenue             DECIMAL(18,4) NOT NULL,
    product_cost            DECIMAL(18,4) NOT NULL,
    shipping_cost_allocated DECIMAL(18,4) NOT NULL,
    contribution_margin     DECIMAL(18,4) NOT NULL,
    -- Số liệu chuẩn hóa quy đổi sang USD (chuẩn phục vụ OLAP tổng hợp đa quốc gia):
    gross_sales_usd         DECIMAL(18,4) NOT NULL,
    net_revenue_usd         DECIMAL(18,4) NOT NULL,
    product_cost_usd        DECIMAL(18,4) NOT NULL,
    contribution_margin_usd DECIMAL(18,4) NOT NULL,
    CONSTRAINT PK_Fact_Sales PRIMARY KEY CLUSTERED (order_id, product_key),
    CONSTRAINT FK_FSales_Date     FOREIGN KEY (date_key)          REFERENCES dbo.Dim_Date (date_key),
    CONSTRAINT FK_FSales_Cust     FOREIGN KEY (customer_key)      REFERENCES dbo.Dim_Customer (customer_key),
    CONSTRAINT FK_FSales_Prod     FOREIGN KEY (product_key)       REFERENCES dbo.Dim_Product (product_key),
    CONSTRAINT FK_FSales_Region   FOREIGN KEY (region_key)        REFERENCES dbo.Dim_Region (region_key),
    CONSTRAINT FK_FSales_Profile  FOREIGN KEY (order_profile_key) REFERENCES dbo.Dim_Order_Profile (order_profile_key),
    CONSTRAINT FK_FSales_Mkt      FOREIGN KEY (marketing_key)     REFERENCES dbo.Dim_Marketing_Campaign (marketing_key),
    CONSTRAINT FK_FSales_Currency FOREIGN KEY (currency_key)      REFERENCES dbo.Dim_Currency (currency_key)
);
GO

-- 5.2 Fact_Order_Fulfillment (Accumulating Snapshot Fact - Grain: 1 đơn hàng)
CREATE TABLE dbo.Fact_Order_Fulfillment (
    order_id                   VARCHAR(50)   NOT NULL,   -- Degenerate Dimension
    order_date_key             INT           NOT NULL,   -- Role: Ngày đặt
    expected_delivery_date_key INT           NOT NULL,   -- Role: Ngày dự kiến
    actual_delivery_date_key   INT           NOT NULL,   -- Role: Ngày nhận thực tế (-1 nếu chưa giao)
    customer_key               INT           NOT NULL,
    region_key                 INT           NULL,
    shipping_key               INT           NULL,
    order_profile_key          INT           NULL,
    currency_key               INT           NOT NULL,
    delivery_days              SMALLINT      NOT NULL,
    estimated_delivery_days    SMALLINT      NOT NULL,
    delivery_variance_days     SMALLINT      NOT NULL,   -- delivery_days - estimated_delivery_days
    late_flag                  TINYINT       NOT NULL,   -- 1 nếu trễ hạn
    shipping_cost              DECIMAL(18,4) NOT NULL,
    order_net_revenue          DECIMAL(18,4) NOT NULL,
    shipping_cost_usd          DECIMAL(18,4) NOT NULL,
    order_net_revenue_usd      DECIMAL(18,4) NOT NULL,
    CONSTRAINT PK_Fact_Order_Fulfillment PRIMARY KEY CLUSTERED (order_id),
    CONSTRAINT CK_FFulfil_late_flag CHECK (late_flag IN (0, 1)),
    CONSTRAINT FK_FFulfil_OrderDate  FOREIGN KEY (order_date_key)             REFERENCES dbo.Dim_Date (date_key),
    CONSTRAINT FK_FFulfil_ExpDate    FOREIGN KEY (expected_delivery_date_key) REFERENCES dbo.Dim_Date (date_key),
    CONSTRAINT FK_FFulfil_ActDate    FOREIGN KEY (actual_delivery_date_key)   REFERENCES dbo.Dim_Date (date_key),
    CONSTRAINT FK_FFulfil_Cust       FOREIGN KEY (customer_key)               REFERENCES dbo.Dim_Customer (customer_key),
    CONSTRAINT FK_FFulfil_Region     FOREIGN KEY (region_key)                 REFERENCES dbo.Dim_Region (region_key),
    CONSTRAINT FK_FFulfil_Ship       FOREIGN KEY (shipping_key)               REFERENCES dbo.Dim_Shipping (shipping_key),
    CONSTRAINT FK_FFulfil_Profile    FOREIGN KEY (order_profile_key)          REFERENCES dbo.Dim_Order_Profile (order_profile_key),
    CONSTRAINT FK_FFulfil_Currency   FOREIGN KEY (currency_key)               REFERENCES dbo.Dim_Currency (currency_key)
);
GO

-- 5.3 Fact_Return (Transaction Fact - Grain: 1 đơn hàng bị hoàn trả)
CREATE TABLE dbo.Fact_Return (
    order_id                  VARCHAR(50)   NOT NULL,   -- Degenerate Dimension
    date_key                  INT           NOT NULL,   -- Ngày đặt ban đầu
    return_date_key           INT           NOT NULL,   -- Ngày tiến hành hoàn trả
    customer_key              INT           NOT NULL,
    region_key                INT           NULL,
    shipping_key              INT           NULL,
    return_profile_key        INT           NULL,
    currency_key              INT           NOT NULL,
    returned_net_revenue      DECIMAL(18,4) NOT NULL,
    returned_product_cost     DECIMAL(18,4) NOT NULL,
    return_margin_impact      DECIMAL(18,4) NOT NULL,   -- Số âm phản ánh mức giảm lợi nhuận
    returned_net_revenue_usd  DECIMAL(18,4) NOT NULL,
    return_margin_impact_usd  DECIMAL(18,4) NOT NULL,
    CONSTRAINT PK_Fact_Return PRIMARY KEY CLUSTERED (order_id),
    CONSTRAINT FK_FReturn_Date       FOREIGN KEY (date_key)           REFERENCES dbo.Dim_Date (date_key),
    CONSTRAINT FK_FReturn_ReturnDate FOREIGN KEY (return_date_key)    REFERENCES dbo.Dim_Date (date_key),
    CONSTRAINT FK_FReturn_Cust       FOREIGN KEY (customer_key)       REFERENCES dbo.Dim_Customer (customer_key),
    CONSTRAINT FK_FReturn_Region     FOREIGN KEY (region_key)         REFERENCES dbo.Dim_Region (region_key),
    CONSTRAINT FK_FReturn_Ship       FOREIGN KEY (shipping_key)       REFERENCES dbo.Dim_Shipping (shipping_key),
    CONSTRAINT FK_FReturn_Profile    FOREIGN KEY (return_profile_key) REFERENCES dbo.Dim_Return_Profile (return_profile_key),
    CONSTRAINT FK_FReturn_Currency   FOREIGN KEY (currency_key)       REFERENCES dbo.Dim_Currency (currency_key)
);
GO

-- 5.4 Fact_Customer_Feedback (Event Metric Fact - Grain: 1 đánh giá của khách)
CREATE TABLE dbo.Fact_Customer_Feedback (
    order_id            VARCHAR(50)  NOT NULL,   -- Degenerate Dimension
    date_key            INT          NOT NULL,
    customer_key        INT          NOT NULL,
    region_key          INT          NULL,
    order_profile_key   INT          NULL,
    rating_value        DECIMAL(5,2) NOT NULL,
    review_sentiment    VARCHAR(50)  NULL,
    review_length       INT          NULL,
    is_negative_flag    BIT          NOT NULL,   -- 1 nếu rating <= 2.0 hoặc Negative
    review_count        TINYINT      NOT NULL DEFAULT 1,
    CONSTRAINT PK_Fact_Customer_Feedback PRIMARY KEY CLUSTERED (order_id),
    CONSTRAINT FK_FFeedback_Date    FOREIGN KEY (date_key)          REFERENCES dbo.Dim_Date (date_key),
    CONSTRAINT FK_FFeedback_Cust    FOREIGN KEY (customer_key)      REFERENCES dbo.Dim_Customer (customer_key),
    CONSTRAINT FK_FFeedback_Region  FOREIGN KEY (region_key)        REFERENCES dbo.Dim_Region (region_key),
    CONSTRAINT FK_FFeedback_Profile FOREIGN KEY (order_profile_key) REFERENCES dbo.Dim_Order_Profile (order_profile_key)
);
GO

-- 5.5 Fact_Customer_Acquisition (First-Event Fact - Grain: 1 khách hàng tại đơn đầu tiên)
CREATE TABLE dbo.Fact_Customer_Acquisition (
    customer_key                 INT           NOT NULL,
    date_key                     INT           NOT NULL,   -- Ngày phát sinh đơn đầu tiên
    region_key                   INT           NULL,
    marketing_key                INT           NULL,       -- Kênh thu hút đầu tiên
    currency_key                 INT           NOT NULL,
    customer_acquisition_cost    DECIMAL(18,4) NOT NULL,   -- CAC
    first_order_net_revenue      DECIMAL(18,4) NOT NULL,
    customer_acquisition_cost_usd DECIMAL(18,4) NOT NULL,
    first_order_net_revenue_usd  DECIMAL(18,4) NOT NULL,
    new_customer_count           TINYINT       NOT NULL DEFAULT 1,
    CONSTRAINT PK_Fact_Customer_Acquisition PRIMARY KEY CLUSTERED (customer_key),
    CONSTRAINT FK_FAcq_Cust     FOREIGN KEY (customer_key)  REFERENCES dbo.Dim_Customer (customer_key),
    CONSTRAINT FK_FAcq_Date     FOREIGN KEY (date_key)      REFERENCES dbo.Dim_Date (date_key),
    CONSTRAINT FK_FAcq_Region   FOREIGN KEY (region_key)    REFERENCES dbo.Dim_Region (region_key),
    CONSTRAINT FK_FAcq_Mkt      FOREIGN KEY (marketing_key) REFERENCES dbo.Dim_Marketing_Campaign (marketing_key),
    CONSTRAINT FK_FAcq_Currency FOREIGN KEY (currency_key)  REFERENCES dbo.Dim_Currency (currency_key)
);
GO

-- 5.6 Fact_Customer_Snapshot (Periodic Snapshot - Grain: 1 khách hàng x 1 cuối tháng)
CREATE TABLE dbo.Fact_Customer_Snapshot (
    snapshot_date_key          INT           NOT NULL,   -- Ngày chụp cuối tháng (Dim_Date)
    customer_key               INT           NOT NULL,
    cumulative_net_revenue_usd DECIMAL(18,4) NOT NULL,   -- Doanh số trọn đời lũy kế (LTV)
    cumulative_order_count     INT           NOT NULL,
    cumulative_points_earned   INT           NOT NULL,
    cumulative_points_redeemed INT           NOT NULL,
    active_90d_flag            TINYINT       NOT NULL,
    customer_count             TINYINT       NOT NULL DEFAULT 1,
    month_net_revenue_usd      DECIMAL(18,4) NOT NULL,
    month_order_count          INT           NOT NULL,
    days_since_last_order      INT           NOT NULL,   -- Recency (R)
    rfm_score                  TINYINT       NOT NULL,   -- 1-5
    loyalty_tier               VARCHAR(50)   NOT NULL,   -- Bronze, Silver, Gold, Platinum
    CONSTRAINT PK_Fact_Customer_Snapshot PRIMARY KEY CLUSTERED (snapshot_date_key, customer_key),
    CONSTRAINT CK_FSnap_active_flag CHECK (active_90d_flag IN (0, 1)),
    CONSTRAINT CK_FSnap_rfm CHECK (rfm_score BETWEEN 1 AND 5),
    CONSTRAINT FK_FSnap_Date FOREIGN KEY (snapshot_date_key) REFERENCES dbo.Dim_Date (date_key),
    CONSTRAINT FK_FSnap_Cust FOREIGN KEY (customer_key)      REFERENCES dbo.Dim_Customer (customer_key)
);
GO

-- 5.7 Fact_Campaign_Promotion (Transaction Fact - Grain: 1 đơn hàng tham gia khuyến mãi)
CREATE TABLE dbo.Fact_Campaign_Promotion (
    order_id            VARCHAR(50)   NOT NULL,   -- Degenerate Dimension
    date_key            INT           NOT NULL,
    customer_key        INT           NOT NULL,
    marketing_key       INT           NOT NULL,
    region_key          INT           NULL,
    order_profile_key   INT           NULL,
    currency_key        INT           NOT NULL,
    gross_sales         DECIMAL(18,4) NOT NULL,
    discount_amount     DECIMAL(18,4) NOT NULL,
    net_revenue         DECIMAL(18,4) NOT NULL,
    gross_sales_usd     DECIMAL(18,4) NOT NULL,
    discount_amount_usd DECIMAL(18,4) NOT NULL,
    net_revenue_usd     DECIMAL(18,4) NOT NULL,
    discount_rate       DECIMAL(18,8) NOT NULL,   -- discount_amount / gross_sales
    is_coupon_order     BIT           NOT NULL,   -- 1 = Có nhập mã voucher
    discount_type       VARCHAR(30)   NOT NULL DEFAULT 'PROMOTION', -- COUPON, AUTO_DISCOUNT, STANDARD
    CONSTRAINT PK_Fact_Campaign_Promotion PRIMARY KEY CLUSTERED (order_id),
    CONSTRAINT FK_FPromo_Date     FOREIGN KEY (date_key)          REFERENCES dbo.Dim_Date (date_key),
    CONSTRAINT FK_FPromo_Cust     FOREIGN KEY (customer_key)      REFERENCES dbo.Dim_Customer (customer_key),
    CONSTRAINT FK_FPromo_Mkt      FOREIGN KEY (marketing_key)     REFERENCES dbo.Dim_Marketing_Campaign (marketing_key),
    CONSTRAINT FK_FPromo_Region   FOREIGN KEY (region_key)        REFERENCES dbo.Dim_Region (region_key),
    CONSTRAINT FK_FPromo_Profile  FOREIGN KEY (order_profile_key) REFERENCES dbo.Dim_Order_Profile (order_profile_key),
    CONSTRAINT FK_FPromo_Currency FOREIGN KEY (currency_key)      REFERENCES dbo.Dim_Currency (currency_key)
);
GO


-- =====================================================================
-- 6. TẠO INDEXES TỐI ƯU HIỆU NĂNG CHO SSAS CUBE VÀ TRUY VẤN OLAP
-- =====================================================================
CREATE NONCLUSTERED INDEX IX_FSales_date       ON dbo.Fact_Sales (date_key);
CREATE NONCLUSTERED INDEX IX_FSales_customer   ON dbo.Fact_Sales (customer_key);
CREATE NONCLUSTERED INDEX IX_FSales_product    ON dbo.Fact_Sales (product_key);
CREATE NONCLUSTERED INDEX IX_FSales_currency   ON dbo.Fact_Sales (currency_key);
CREATE NONCLUSTERED INDEX IX_FSales_region     ON dbo.Fact_Sales (region_key);
CREATE NONCLUSTERED INDEX IX_FSales_profile    ON dbo.Fact_Sales (order_profile_key);
CREATE NONCLUSTERED INDEX IX_FSales_marketing  ON dbo.Fact_Sales (marketing_key);

CREATE NONCLUSTERED INDEX IX_FFulfil_orderdate ON dbo.Fact_Order_Fulfillment (order_date_key);
CREATE NONCLUSTERED INDEX IX_FFulfil_actdate   ON dbo.Fact_Order_Fulfillment (actual_delivery_date_key);
CREATE NONCLUSTERED INDEX IX_FFulfil_customer  ON dbo.Fact_Order_Fulfillment (customer_key);
CREATE NONCLUSTERED INDEX IX_FFulfil_shipping  ON dbo.Fact_Order_Fulfillment (shipping_key);

CREATE NONCLUSTERED INDEX IX_FReturn_date      ON dbo.Fact_Return (date_key);
CREATE NONCLUSTERED INDEX IX_FReturn_retdate   ON dbo.Fact_Return (return_date_key);
CREATE NONCLUSTERED INDEX IX_FReturn_customer  ON dbo.Fact_Return (customer_key);

CREATE NONCLUSTERED INDEX IX_FFeedback_date    ON dbo.Fact_Customer_Feedback (date_key);
CREATE NONCLUSTERED INDEX IX_FFeedback_cust    ON dbo.Fact_Customer_Feedback (customer_key);

CREATE NONCLUSTERED INDEX IX_FAcq_date         ON dbo.Fact_Customer_Acquisition (date_key);
CREATE NONCLUSTERED INDEX IX_FAcq_customer     ON dbo.Fact_Customer_Acquisition (customer_key);

CREATE NONCLUSTERED INDEX IX_FSnap_date        ON dbo.Fact_Customer_Snapshot (snapshot_date_key);
CREATE NONCLUSTERED INDEX IX_FSnap_customer    ON dbo.Fact_Customer_Snapshot (customer_key);

CREATE NONCLUSTERED INDEX IX_FPromo_date       ON dbo.Fact_Campaign_Promotion (date_key);
CREATE NONCLUSTERED INDEX IX_FPromo_mkt        ON dbo.Fact_Campaign_Promotion (marketing_key);

CREATE NONCLUSTERED INDEX IX_Reject_batch      ON dbo.etl_reject (batch_id);
CREATE NONCLUSTERED INDEX IX_Reject_cat        ON dbo.etl_reject (reject_category);
GO

PRINT N'=====================================================================';
PRINT N'ĐÃ HOÀN TẤT THIẾT LẬP DATABASE VÀ SCHEMA ECOMMERCEDW MỚI (01)';
PRINT N'GỒM 9 DIMENSIONS + 7 FACTS + HỆ THỐNG KIỂM TOÁN ETL METADATA.';
PRINT N'=====================================================================';
GO
