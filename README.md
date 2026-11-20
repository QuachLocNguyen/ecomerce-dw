# Ecommerce Data Warehouse & Analytics Platform

> **End-to-end Data Engineering capstone project** — Galaxy-Schema DWH on SQL Server with SSIS ETL pipeline, SSAS OLAP Cubes, and a Near Real-Time Micro-Batch streaming layer simulating continuous order ingestion.

![Python](https://img.shields.io/badge/Python-3.8+-3776AB?logo=python&logoColor=white)
![SQL Server](https://img.shields.io/badge/SQL_Server-2019-CC2927?logo=microsoftsqlserver&logoColor=white)
![SSIS](https://img.shields.io/badge/SSIS-ETL_Pipeline-0078D4?logo=microsoft&logoColor=white)
![SSAS](https://img.shields.io/badge/SSAS-OLAP_Cubes-0078D4?logo=microsoft&logoColor=white)
![T-SQL](https://img.shields.io/badge/T--SQL-MDX-CC2927)

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  SOURCE                 STAGING              DWH                  │
│  4 CSV files      →     4 stg_* tables  →   9 Dimensions         │
│  138K orders            (SQL Server)         7 Fact Tables        │
│  397K line-items                             EcommerceDW          │
│  25K customers                                                     │
│  1.1K products                                   ↓                │
│                                             SSAS OLAP             │
│                                             4 Subject Cubes       │
├──────────────────────────────────────────────────────────────────┤
│  STREAMING LAYER (Near Real-Time)                                  │
│  stream_producer.py → stg_sales_stream → sp_Consume_MicroBatch   │
│  (Python pyodbc)       (watermark buffer)   (SQL Agent, 30s)      │
└──────────────────────────────────────────────────────────────────┘
```

---

## Business Questions Answered

This DWH is designed to answer **3 core business questions** that drive real operational decisions:

---

### ❓ BQ1 — Does discounting destroy profit margins?
> *"Which discount tiers (0%, 1-10%, 11-25%, 26-50%, >50%) still produce positive contribution margin — and which are eroding profitability?"*

**Decision supported:** Should we continue deep-discount campaigns (>50%)? Which product categories are most vulnerable to margin erosion?

**Queries:** `03_Queries_SQL.sql` → BQ1-A, BQ1-B · `04_Queries_MDX.mdx` → MDX BQ1-A, BQ1-B, BQ1-C

<!-- 📸 Chèn ảnh kết quả query BQ1-A: bảng Discount Tier vs Margin Rate % -->
<!-- Ví dụ: nhóm >50% có margin_rate_pct thấp nhất → chứng minh giảm giá sâu không hiệu quả -->
<!-- ![BQ1 - Discount vs Margin](docs/screenshots/bq1_discount_margin.png) -->

---

### ❓ BQ2 — Which customers are worth keeping? (RFM + CLV + Churn Risk)
> *"At the latest monthly snapshot: which Platinum/Gold customers have high CLV but haven't purchased in 90+ days (churn risk)?"*

**Decision supported:** Which loyalty tier needs an immediate retention campaign? How to allocate the retention budget across Bronze → Platinum tiers?

**Queries:** `03_Queries_SQL.sql` → BQ2-A, BQ2-B · `04_Queries_MDX.mdx` → MDX BQ2-A, BQ2-B, BQ2-C

<!-- 📸 Chèn ảnh kết quả query BQ2-A: bảng Loyalty Tier × Churn Risk % × Avg CLV USD -->
<!-- ![BQ2 - Churn Risk by Loyalty Tier](docs/screenshots/bq2_churn_clv.png) -->

---

### ❓ BQ3 — Which marketing channel has the best ROI?
> *"Comparing Customer Acquisition Cost (CAC) vs First-Order Net Revenue by channel — which channel acquires the most valuable customers at the lowest cost?"*

**Decision supported:** Where to shift next month's marketing budget? Which channel type (Paid Media / Owned / Organic) delivers sustainable ROI?

**Queries:** `03_Queries_SQL.sql` → BQ3-A, BQ3-B · `04_Queries_MDX.mdx` → MDX BQ3-A, BQ3-B, BQ3-C

<!-- 📸 Chèn ảnh kết quả query BQ3-A: bảng Marketing Channel × CAC × ROI Ratio -->
<!-- ![BQ3 - Marketing ROI by Channel](docs/screenshots/bq3_marketing_roi.png) -->

---



| Fact Table | Rows | Description |
|---|---|---|
| `Fact_Sales` | **397,482** | Core transaction grain: 1 line-item per order |
| `Fact_Order_Fulfillment` | 113,559 | Accumulating snapshot: delivery tracking |
| `Fact_Customer_Snapshot` | 131,716 | Periodic snapshot: monthly RFM + loyalty tier |
| `Fact_Customer_Feedback` | 113,559 | Customer review & sentiment per order |
| `Fact_Customer_Acquisition` | 24,911 | First-event fact: CAC per new customer |
| `Fact_Campaign_Promotion` | 14,986 | Coupon & promotion transaction fact |
| `Fact_Return` | 9,462 | Return transaction fact |
| **Total Net Revenue (USD)** | **~\$192M** | Multi-currency, normalized to USD |

---

## Key Features

### 1. Galaxy Schema — 9 Dimensions + 7 Facts

| Category | Tables |
|---|---|
| **Core Facts** | Fact_Sales, Fact_Order_Fulfillment, Fact_Return |
| **Strategic Facts** | Fact_Customer_Feedback, Fact_Customer_Acquisition, Fact_Customer_Snapshot, Fact_Campaign_Promotion |
| **Dimensions** | Dim_Date (role-playing), Dim_Customer **(SCD Type 2)**, Dim_Product (SCD Type 1), Dim_Region, Dim_Shipping, Dim_Order_Profile **(Junk)**, Dim_Marketing_Campaign, Dim_Return_Profile, Dim_Currency |

### 2. SSIS ETL Pipeline — 5-Container Design

Architected a 5-container SSIS package with parallel CSV ingestion, SCD Type 2 customer upsert, 6-category error quarantine, and watermark-based incremental load — processing ~800K rows in a single batch run.

| Container | Tasks | Pattern |
|---|---|---|
| **C00 — Prepare** | Truncate staging → Begin batch → Seed unknown keys + 7 FX rates | Sequential |
| **C01 — Extract** | 4× Flat File → OLE DB Fast Load | **Parallel** |
| **C02 — Quarantine** | Classify errors into `etl_reject` (NULL/TYPE/BUSINESS/ORPHAN/DUP/RANGE) | Sequential |
| **C03 — Load Dims** | 7× Dim SP (SCD2 Customer, SCD1 Product, SCD0 others) | **Parallel** |
| **C04 — Load Facts** | 7× Fact SP with TRY…CATCH isolation | **Parallel** |
| **C05 — Close** | Update watermark → Close batch SUCCESS | Sequential |

<!-- 📸 Chèn ảnh SSIS Package (tất cả containers xanh lá sau khi chạy xong) -->
<!-- ![SSIS ETL Pipeline](docs/screenshots/ssis_package_success.png) -->

### 3. Multi-Currency Support

- **7 currencies:** USD, EUR, GBP, CAD, AUD, INR, AED
- All monetary facts store **dual columns**: original amount + `_usd` normalized via `Dim_Currency.exchange_rate_to_usd`
- Enables cross-country revenue aggregation in SSAS without data duplication

### 4. SSAS — 4 Subject-Area OLAP Cubes

| Cube | Measure Groups | Sample Calculated Measures |
|---|---|---|
| **Doanh thu & Bán hàng** | Fact_Sales, Fact_Return, Fact_Campaign | `Margin Rate %`, `Discount Rate %`, `Return Revenue Rate %`, `Coupon Usage Rate %` |
| **Van hanh Logistics** | Fact_Order_Fulfillment | `Late Delivery Rate %`, `Avg Delivery Delay (days)` |
| **Hanh vi Khach hang** | Fact_Feedback, Fact_Acquisition, Fact_Snapshot | `Average CAC USD`, `Negative Review Rate %`, `Points Redemption Rate %` |
| **Tong hop Audit** | All 7 facts | Base measures — cross-fact reconciliation |

<!-- 📸 Chèn ảnh SSAS Cube Browser hoặc Excel PivotTable kết nối đến cube -->
<!-- ![SSAS Cube Browser](docs/screenshots/ssas_cube_browser.png) -->

### 5. Near Real-Time Streaming Layer

```
stream_producer.py          stg_sales_stream          sp_Consume_MicroBatch
─────────────────           ────────────────          ─────────────────────
• Reads 138K CSV rows       • BIGINT stream_id        • Watermark: stream_id
• Shuffles for realism      • DATETIME2(3) ingest_time  > last processed
• Inserts in configurable   • is_processed BIT        • Marks batch → inserts
  batches (default: 100)    • Partial index on          into Fact_Sales
• Reports rows/sec stats      unprocessed rows        • Idempotent: NOT EXISTS
                                                      • SQL Agent Job: every 30s
```

<!-- 📸 Chèn ảnh terminal Python hiển thị throughput (rows/sec) -->
<!-- ![Stream Producer Output](docs/screenshots/stream_producer_output.png) -->

---

## Project Structure

```
ecomdw/
├── 00_Create_Staging_Tables.sql    # Staging DDL — 4 stg_* buffer tables
├── 01_Create_EcommerceDW_v2.sql    # DWH DDL — 9 Dims + 7 Facts + ETL metadata
├── 02_Streaming_Setup.sql          # Streaming DDL — buffer table + SP + watermark
├── stream_producer.py              # Python micro-batch event producer
├── stream_consumer.py              # Python consumer / monitor
├── du_lieu_csv/                    # Source data (4 CSV files)
│   ├── ecommerce_sales_customer_analytics_150k.csv   # 138K orders
│   ├── order_items.csv                               # 397K line-items
│   ├── product_catalog.csv                           # 1.1K products
│   └── customer_master.csv                           # 25K customers
├── SSIS_ETL_Project/               # Visual Studio SSIS Package (.dtsx)
└── SSAS_OLAP_Project/              # Visual Studio SSAS Project
    └── Ecom/                       # 4 cubes (.cube) + 9 dims (.dim)
```

---

## Setup & Run

### Prerequisites
- SQL Server 2019 + SSMS 18+
- Visual Studio 2019/2022 with **SSIS** and **SSAS** extensions
- Python 3.8+ with `pyodbc`

### Steps

```powershell
# 1. Create DWH schema (run in SSMS in order)
#    00_Create_Staging_Tables.sql
#    01_Create_EcommerceDW_v2.sql
#    02_Streaming_Setup.sql

# 2. Run ETL pipeline
#    Open SSIS_ETL_Project → Visual Studio → Run Package (F5)

# 3. Deploy SSAS Cubes
#    Open SSAS_OLAP_Project → Build → Deploy → Process Full

# 4. Run streaming simulator
pip install pyodbc
python stream_producer.py --batch-size 100 --delay 0.5

# Then consume in SSMS:
# EXEC dbo.sp_Consume_MicroBatch @batch_size = 5000;
# SELECT * FROM dbo.vw_Stream_Dashboard;
```

---

## Sample Queries

### SQL — Top Countries by Net Revenue

```sql
SELECT
    r.country,
    SUM(fs.net_revenue_usd)             AS net_revenue_usd,
    ROUND(SUM(fs.contribution_margin_usd) * 100.0
          / NULLIF(SUM(fs.net_revenue_usd), 0), 2) AS margin_rate_pct
FROM dbo.Fact_Sales fs
JOIN dbo.Dim_Region r ON fs.region_key = r.region_key
GROUP BY r.country
ORDER BY net_revenue_usd DESC;
```

### MDX — Margin Rate % by Country (SSAS)

```mdx
SELECT
  {[Measures].[Net Revenue USD], [Measures].[Margin Rate %]} ON COLUMNS,
  [Dim Region].[Country].[Country].MEMBERS ON ROWS
FROM [Doanh Thu Ban Hang];
```

---

## Skills Demonstrated

| Area | Skills |
|---|---|
| **Data Modeling** | Kimball Galaxy Schema, SCD Type 1 & 2, Junk Dimension, Role-Playing Dimension, Degenerate Dimension |
| **ETL Engineering** | SSIS parallel containers, error quarantine (6 categories), watermark incremental load, idempotent design |
| **OLAP** | SSAS Multidimensional, MDX calculated members, subject-area cube decomposition |
| **Streaming** | Micro-batch pattern, watermark timestamp, Python producer → T-SQL consumer |
| **SQL** | T-SQL window functions, SCD2 MERGE, TRY…CATCH isolation, multi-currency normalization |
| **Python** | pyodbc, csv.DictReader, argparse CLI, throughput monitoring |

---

## Dataset

Source: [Kaggle — E-Commerce Sales & Customer Analytics](https://www.kaggle.com) *(update link here)*

> 138,117 orders · 397,482 line-items · 25,000 customers · 1,175 products · 2021–2025 · 7 countries · 7 currencies
