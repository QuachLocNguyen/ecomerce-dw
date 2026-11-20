-- =====================================================================
-- FILE: 02_Streaming_Setup.sql
-- MỤC ĐÍCH: Thiết lập cơ chế Near Real-Time Streaming (Micro-Batch)
-- ĐẶC TÍNH:
--   1. Bảng đệm stg_sales_stream (buffer nhận sự kiện từ Python Producer)
--   2. Bảng watermark theo timestamp (ETL_Stream_Watermark)
--   3. Stored Procedure tiêu thụ micro-batch theo Sliding Window Timestamp
-- CÁCH VẬN HÀNH:
--   - stream_producer.py phát sự kiện liên tục vào stg_sales_stream
--   - sp_Consume_MicroBatch chạy định kỳ (mỗi 30s ~ 1 phút) hút batch mới
--   - Idempotent: Chạy lại không bao giờ sinh dữ liệu trùng lặp
-- =====================================================================

USE EcommerceDW;
GO

-- =====================================================================
-- BƯỚC 1: TẠO BẢNG ĐỆM NHẬN SỰ KIỆN REAL-TIME (Stream Buffer)
-- =====================================================================

IF OBJECT_ID('dbo.stg_sales_stream', 'U') IS NOT NULL DROP TABLE dbo.stg_sales_stream;
GO

CREATE TABLE dbo.stg_sales_stream (
    -- Metadata kiểm soát luồng streaming (không có trong CSV gốc)
    stream_id               BIGINT IDENTITY(1,1) NOT NULL,  -- Khóa tự tăng để đảm bảo thứ tự sự kiện
    ingest_time             DATETIME2(3)         NOT NULL DEFAULT SYSDATETIME(), -- Mốc thời gian sự kiện đến (ms precision)
    batch_sequence          INT                  NULL,      -- Số thứ tự micro-batch đã xử lý (NULL = chưa hút)
    is_processed            BIT                  NOT NULL DEFAULT 0, -- 0 = Mới, 1 = Đã hút vào Fact

    -- Dữ liệu đơn hàng thô (tất cả để NVARCHAR để không bị lỗi khi nhận sự kiện)
    order_id                NVARCHAR(50)   NULL,
    order_date              NVARCHAR(50)   NULL,
    order_time              NVARCHAR(50)   NULL,
    order_status            NVARCHAR(50)   NULL,
    sales_channel           NVARCHAR(50)   NULL,
    customer_id             NVARCHAR(50)   NULL,
    customer_name           NVARCHAR(150)  NULL,
    customer_age            NVARCHAR(10)   NULL,
    gender                  NVARCHAR(20)   NULL,
    customer_segment        NVARCHAR(50)   NULL,
    customer_type           NVARCHAR(50)   NULL,
    customer_city           NVARCHAR(100)  NULL,
    customer_state          NVARCHAR(100)  NULL,
    customer_country        NVARCHAR(100)  NULL,
    region                  NVARCHAR(50)   NULL,
    customer_postal_code    NVARCHAR(30)   NULL,
    payment_method          NVARCHAR(50)   NULL,
    payment_status          NVARCHAR(50)   NULL,
    currency                NVARCHAR(10)   NULL,
    shipping_method         NVARCHAR(50)   NULL,
    warehouse               NVARCHAR(50)   NULL,
    delivery_days           NVARCHAR(10)   NULL,
    estimated_delivery_days NVARCHAR(10)   NULL,
    delivery_status         NVARCHAR(50)   NULL,
    return_status           NVARCHAR(50)   NULL,
    return_reason           NVARCHAR(150)  NULL,
    customer_rating         NVARCHAR(10)   NULL,
    review_sentiment        NVARCHAR(50)   NULL,
    customer_review         NVARCHAR(1000) NULL,
    marketing_channel       NVARCHAR(100)  NULL,
    campaign_name           NVARCHAR(150)  NULL,
    coupon_code             NVARCHAR(50)   NULL,
    loyalty_points_earned   NVARCHAR(10)   NULL,
    loyalty_points_redeemed NVARCHAR(10)   NULL,
    quantity                NVARCHAR(10)   NULL,
    gross_sales             NVARCHAR(30)   NULL,
    discount_amount         NVARCHAR(30)   NULL,
    tax_amount              NVARCHAR(30)   NULL,
    shipping_cost           NVARCHAR(30)   NULL,
    net_sales               NVARCHAR(30)   NULL,
    product_cost            NVARCHAR(30)   NULL,
    profit                  NVARCHAR(30)   NULL,
    product_id              NVARCHAR(50)   NULL,

    CONSTRAINT PK_stg_sales_stream PRIMARY KEY CLUSTERED (stream_id)
);
GO

-- Index tối ưu: Tìm nhanh các sự kiện chưa xử lý theo thời gian đến
CREATE NONCLUSTERED INDEX IX_Stream_Unprocessed ON dbo.stg_sales_stream (is_processed, ingest_time)
WHERE is_processed = 0;
GO

-- =====================================================================
-- BƯỚC 2: BẢNG WATERMARK THEO TIMESTAMP (ETL_Stream_Watermark)
-- Lưu mốc ingest_time lớn nhất đã được hút vào Fact trong lần chạy gần nhất
-- =====================================================================

IF OBJECT_ID('dbo.ETL_Stream_Watermark', 'U') IS NOT NULL DROP TABLE dbo.ETL_Stream_Watermark;
GO

CREATE TABLE dbo.ETL_Stream_Watermark (
    stream_name          VARCHAR(100) NOT NULL,
    last_processed_time  DATETIME2(3) NOT NULL DEFAULT '2000-01-01 00:00:00.000',
    last_stream_id       BIGINT       NOT NULL DEFAULT 0, -- stream_id cuối cùng đã xử lý
    total_events_in      BIGINT       NOT NULL DEFAULT 0, -- Tổng sự kiện đã nhận
    total_events_out     BIGINT       NOT NULL DEFAULT 0, -- Tổng sự kiện đã hút thành công vào Fact
    total_events_error   BIGINT       NOT NULL DEFAULT 0, -- Tổng sự kiện bị lỗi (cách ly)
    last_run_duration_ms INT          NULL,               -- Thời gian xử lý batch gần nhất (ms)
    updated_at           DATETIME2    NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_ETL_Stream_Watermark PRIMARY KEY CLUSTERED (stream_name)
);
GO

-- Khởi tạo watermark cho stream đơn hàng
INSERT INTO dbo.ETL_Stream_Watermark (stream_name, last_processed_time, last_stream_id)
VALUES ('sales_stream', '2000-01-01 00:00:00.000', 0);
GO

-- =====================================================================
-- BƯỚC 3: STORED PROCEDURE TIÊU THỤ MICRO-BATCH
-- Chạy định kỳ mỗi 30 giây đến 1 phút bởi SQL Server Agent
-- Mỗi lần chạy: Hút tối đa @batch_size sự kiện mới nhất từ stream buffer
-- =====================================================================

CREATE OR ALTER PROCEDURE dbo.sp_Consume_MicroBatch
    @batch_size INT = 5000  -- Kích thước mỗi micro-batch, có thể điều chỉnh
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_time DATETIME2 = SYSDATETIME();
    DECLARE @watermark_time DATETIME2(3);
    DECLARE @watermark_id   BIGINT;
    DECLARE @new_watermark_time DATETIME2(3);
    DECLARE @new_watermark_id   BIGINT;
    DECLARE @rows_processed INT = 0;
    DECLARE @rows_error     INT = 0;
    DECLARE @batch_seq      INT;

    -- 1. Lấy mốc watermark hiện tại
    SELECT
        @watermark_time = last_processed_time,
        @watermark_id   = last_stream_id
    FROM dbo.ETL_Stream_Watermark
    WHERE stream_name = 'sales_stream';

    -- 2. Xác định batch số mấy (để ghi nhận vào cột batch_sequence)
    SELECT @batch_seq = ISNULL(MAX(batch_sequence), 0) + 1
    FROM dbo.stg_sales_stream
    WHERE batch_sequence IS NOT NULL;

    -- 3. Đánh dấu batch sự kiện sẽ xử lý lần này
    --    Chỉ lấy các sự kiện MỚI hơn watermark hiện tại (dùng CTE để hỗ trợ ORDER BY trong UPDATE)
    ;WITH TargetBatch AS (
        SELECT TOP (@batch_size) batch_sequence
        FROM dbo.stg_sales_stream
        WHERE is_processed = 0
          AND stream_id    > @watermark_id
        ORDER BY stream_id ASC  -- Luôn xử lý đúng thứ tự sự kiện
    )
    UPDATE TargetBatch
    SET batch_sequence = @batch_seq;

    IF @@ROWCOUNT = 0
    BEGIN
        PRINT N'Không có sự kiện mới trong luồng. Chờ đợt sau...';
        RETURN;
    END

    -- 4. Nạp batch vừa đánh dấu vào Fact_Sales (kèm quy đổi USD & khử trùng lặp)
    BEGIN TRY
        ;WITH RawEvents AS (
            SELECT
                s.order_id,
                ISNULL(p.product_key, -1) AS product_key,
                ISNULL(CAST(CONVERT(VARCHAR(8), TRY_CONVERT(DATE, s.order_date), 112) AS INT), -1) AS date_key,
                ISNULL(c.customer_key, -1) AS customer_key,
                ISNULL(r.region_key, -1) AS region_key,
                ISNULL(op.order_profile_key, -1) AS order_profile_key,
                ISNULL(mc.marketing_key, -1) AS marketing_key,
                ISNULL(cur.currency_key, -1) AS currency_key,
                TRY_CONVERT(INT, s.quantity) AS quantity,
                TRY_CONVERT(DECIMAL(18,4), s.gross_sales) AS gross_sales,
                TRY_CONVERT(DECIMAL(18,4), s.discount_amount) AS discount_amount,
                TRY_CONVERT(DECIMAL(18,4), s.tax_amount) AS tax_amount,
                TRY_CONVERT(DECIMAL(18,4), s.net_sales) AS net_revenue,
                TRY_CONVERT(DECIMAL(18,4), s.product_cost) AS product_cost,
                TRY_CONVERT(DECIMAL(18,4), s.shipping_cost) AS shipping_cost_allocated,
                TRY_CONVERT(DECIMAL(18,4), s.profit) AS contribution_margin,
                ROUND(TRY_CONVERT(DECIMAL(18,4), s.gross_sales) * ISNULL(cur.exchange_rate_to_usd, 1.0), 4) AS gross_sales_usd,
                ROUND(TRY_CONVERT(DECIMAL(18,4), s.net_sales)   * ISNULL(cur.exchange_rate_to_usd, 1.0), 4) AS net_revenue_usd,
                ROUND(TRY_CONVERT(DECIMAL(18,4), s.product_cost)* ISNULL(cur.exchange_rate_to_usd, 1.0), 4) AS product_cost_usd,
                ROUND(TRY_CONVERT(DECIMAL(18,4), s.profit)      * ISNULL(cur.exchange_rate_to_usd, 1.0), 4) AS contribution_margin_usd,
                ROW_NUMBER() OVER (PARTITION BY s.order_id, ISNULL(p.product_key, -1) ORDER BY s.stream_id ASC) AS rn_fact
            FROM dbo.stg_sales_stream s
            LEFT JOIN dbo.Dim_Product p ON s.product_id = p.product_id
            LEFT JOIN dbo.Dim_Customer c ON s.customer_id = c.customer_id AND c.is_current = 1
            LEFT JOIN dbo.Dim_Region r ON s.region = r.region AND s.customer_state = r.[state] AND s.customer_country = r.country
            LEFT JOIN dbo.Dim_Currency cur ON s.currency = cur.currency_code
            LEFT JOIN dbo.Dim_Order_Profile op ON
                ISNULL(s.order_status, '')    = ISNULL(op.order_status, '') AND
                ISNULL(s.payment_method, '')  = ISNULL(op.payment_method, '') AND
                ISNULL(s.payment_status, '')  = ISNULL(op.payment_status, '') AND
                ISNULL(s.sales_channel, '')   = ISNULL(op.sales_channel, '') AND
                ISNULL(s.delivery_status, '') = ISNULL(op.delivery_status, '')
            LEFT JOIN dbo.Dim_Marketing_Campaign mc ON
                ISNULL(s.marketing_channel, '')         = ISNULL(mc.marketing_channel, '') AND
                ISNULL(s.campaign_name, '')             = ISNULL(mc.campaign_name, '') AND
                ISNULL(s.coupon_code, 'NO_COUPON')      = ISNULL(mc.coupon_code, 'NO_COUPON')
            WHERE s.batch_sequence = @batch_seq
        )
        INSERT INTO dbo.Fact_Sales (
            order_id, product_key, date_key, customer_key, region_key,
            order_profile_key, marketing_key, currency_key,
            quantity, gross_sales, discount_amount, tax_amount,
            net_revenue, product_cost, shipping_cost_allocated, contribution_margin,
            gross_sales_usd, net_revenue_usd, product_cost_usd, contribution_margin_usd
        )
        SELECT
            order_id, product_key, date_key, customer_key, region_key,
            order_profile_key, marketing_key, currency_key,
            quantity, gross_sales, discount_amount, tax_amount,
            net_revenue, product_cost, shipping_cost_allocated, contribution_margin,
            gross_sales_usd, net_revenue_usd, product_cost_usd, contribution_margin_usd
        FROM RawEvents
        WHERE rn_fact = 1
          -- Kiểm tra Idempotent: Bỏ qua nếu order_id đã tồn tại trong Fact
          AND NOT EXISTS (
              SELECT 1 FROM dbo.Fact_Sales fs
              WHERE fs.order_id = RawEvents.order_id AND fs.product_key = RawEvents.product_key
          );

        SET @rows_processed = @@ROWCOUNT;

        -- 5. Đánh dấu các sự kiện đã xử lý thành công
        UPDATE dbo.stg_sales_stream
        SET is_processed = 1
        WHERE batch_sequence = @batch_seq;

        -- 6. Cập nhật Watermark tiến về phía trước
        SELECT
            @new_watermark_time = MAX(ingest_time),
            @new_watermark_id   = MAX(stream_id)
        FROM dbo.stg_sales_stream
        WHERE batch_sequence = @batch_seq;

        UPDATE dbo.ETL_Stream_Watermark
        SET last_processed_time = @new_watermark_time,
            last_stream_id      = @new_watermark_id,
            total_events_out    = total_events_out + @rows_processed,
            last_run_duration_ms = DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            updated_at          = SYSDATETIME()
        WHERE stream_name = 'sales_stream';

        PRINT CONCAT(
            N'[Micro-Batch #', @batch_seq, N'] Đã nạp: ', @rows_processed, N' sự kiện | ',
            N'Watermark tiến đến: ', CONVERT(VARCHAR(26), @new_watermark_time, 121), N' | ',
            N'Thời gian xử lý: ', DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()), N' ms'
        );
    END TRY
    BEGIN CATCH
        SET @rows_error = @@ROWCOUNT;
        UPDATE dbo.ETL_Stream_Watermark
        SET total_events_error = total_events_error + @rows_error,
            updated_at         = SYSDATETIME()
        WHERE stream_name = 'sales_stream';

        PRINT CONCAT(N'LỖI Micro-Batch #', @batch_seq, N': ', ERROR_MESSAGE());
    END CATCH
END;
GO

-- =====================================================================
-- BƯỚC 4: DASHBOARD GIÁM SÁT STREAM (Dùng để demo khi bảo vệ)
-- =====================================================================

CREATE OR ALTER VIEW dbo.vw_Stream_Dashboard AS
SELECT
    w.stream_name,
    w.last_processed_time AS watermark_moc_gan_nhat,
    w.last_stream_id      AS stream_id_cuoi_cung,
    w.total_events_in     AS tong_su_kien_da_nhan,
    w.total_events_out    AS tong_su_kien_da_nap,
    w.total_events_error  AS tong_su_kien_loi,
    w.last_run_duration_ms AS thoi_gian_batch_ms,
    -- Số sự kiện đang chờ trong buffer
    (SELECT COUNT(*) FROM dbo.stg_sales_stream WHERE is_processed = 0) AS su_kien_dang_cho,
    -- Thống kê tốc độ throughput
    w.total_events_out AS tong_da_nap_vao_fact,
    (SELECT COUNT(*) FROM dbo.Fact_Sales) AS tong_fact_sales_hien_tai
FROM dbo.ETL_Stream_Watermark w;
GO

PRINT N'=====================================================================';
PRINT N'ĐÃ TẠO XONG CƠ SỞ HẠ TẦNG STREAMING:';
PRINT N'  - Bảng đệm stg_sales_stream (buffer nhận sự kiện từ Python)';
PRINT N'  - Bảng watermark ETL_Stream_Watermark (theo dõi tiến độ)';
PRINT N'  - sp_Consume_MicroBatch (Consumer tiêu thụ theo Micro-Batch)';
PRINT N'  - vw_Stream_Dashboard (Dashboard giám sát real-time)';
PRINT N'Chạy Python stream_producer.py để bắt đầu phát sự kiện!';
PRINT N'=====================================================================';
GO

-- 1. Tạo User cho tài khoản dịch vụ SSAS trong database EcommerceDW
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = N'NT SERVICE\MSOLAP$SQLEXPRESS')
BEGIN
    CREATE USER [NT SERVICE\MSOLAP$SQLEXPRESS] FOR LOGIN [NT SERVICE\MSOLAP$SQLEXPRESS];
END
GO

-- 2. Cấp quyền đọc dữ liệu (db_datareader) cho SSAS đọc các bảng Dim và Fact
ALTER ROLE [db_datareader] ADD MEMBER [NT SERVICE\MSOLAP$SQLEXPRESS];
GO