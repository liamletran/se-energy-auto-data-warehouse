# Nordic Energy Market Analytics Pipeline

> End-to-end analytics pipeline combining hourly ENTSO-E 
> to model energy cost exposure for Swedish industrial manufacturers.





## Business Context

The Nordic energy market is characterized by high volatility in electricity prices, driven by factors such as weather conditions, fuel costs, and grid constraints. For energy-intensive industries like automotive manufacturing, this volatility can lead to significant cost fluctuations and margin pressure. By building a data pipeline that ingests hourly market data, transforms it into actionable insights, and serves it through a dashboard, we can help manufacturers optimize their energy procurement strategies, identify cost-saving opportunities, and ultimately improve their financial performance in a competitive market.

## Live Dashboard
[]

## Architecture

The pipeline follows a modern **ELT (Extract-Load-Transform)** pattern:

1. **Ingestion (Extract & Load)**: 

Python + Dagster fetches hourly data from APIs, reshapes it (Wide to Long), and loads it into DuckDB Raw schema.

2. **Transformation (Transform)**: 

dbt takes over to clean, join, and model the data into Staging, Intermediate, and Mart layers.

3. **Orchestration**: 

Dagster coordinates the entire flow, ensuring dbt runs only after successful ingestion.

The pipeline is designed to be modular, scalable, and maintainable, allowing for easy addition of new data sources or transformation logic as business needs evolve.

![Pipeline](images/overview_pipeline-Page-1.drawio.gif)

## Stack
| Layer | Tool | Reason |
|---|---|---|
| Orchestration | Dagster | Coordinates partitioned hourly assets, manages retry logic, and triggers Slack alerts |
| Transformation | dbt + DuckDB + DBeaver | SQL-first, version-controlled modelling with a high-performance in-process OLAP warehouse |
| Serving | Evidence.dev | Code-based, Markdown+SQL driven BI layer for rapid, git-integrated dashboard deployment |
| Monitoring | Slack | Proactive, real-time alerting system linked with Dagster to minimize pipeline downtime |




## Data Sources
| Source | Frequency | Method |
|---|---|---|
| **ENTSO-E Transparency Platform** | Hourly | REST API | Raw electricity prices and generation mix for SE3 zone. |
| **Manual Reference Data** | Static | dbt seed (CSV) | Domain-specific lookup tables: OEE proxies, unit conversions, and NACE industry codes. |
| **SCB Industrial Production Index** | Monthly | REST API | Industrial Production Index (IPI) for manufacturing sectors. |
| **Frankfurter.dev Exchange Rates** | Daily | REST API | EUR to SEK exchange rates for cost modeling. |

## Key Design Decisions

- **ELT Architecture**: 
Separation of ingestion (Dagster) and transformation (dbt) allows for modularity and scalability. Ingestion focuses on fetching and loading raw data, while dbt handles all transformations and business logic.

- **Dagster**: Orchestrates the workflow, ensuring that data is ingested and transformed in the correct sequence. dbt handles all transformations, allowing for modular SQL development and testing. DuckDB serves as a local analytical warehouse, providing fast query performance for both dbt and the dashboard.

- **DuckDB as Warehouse**: 
DuckDB is used as a local analytical warehouse that Supports SQL and integrates well with dbt and thus is chosen for its simplicity and performance for local analytics.

- **Evidence.dev for Serving**: Evidence.dev is chosen for its seamless integration with SQL and Markdown, allowing for rapid dashboard development and deployment directly from the codebase, ensuring version control and collaboration.  

- **Proactive Monitoring**: Integration of Slack alerts with Dagster ensures that any pipeline failures are immediately communicated to the team, minimizing downtime and ensuring data reliability for decision-making.


## Definition of Done
- [x] 100% dbt tests pass 
- [x] Idempotent ingestion process
- [x] Source freshness monitoring configured
- [x] Live dashboard deployed at

## How to Run


















Giải thích tại sao dùng df.melt() giống transformation nhê
query_generation() của entsoe-py trả về DataFrame dạng wide
datetime             | Hydro Water Reservoir | Wind Onshore | Nuclear
2026-05-04 00:00:00  | 1250.0                | 890.0        | 2100.0
2026-05-04 01:00:00  | 1180.0                | 920.0        | 2100.0
Sau melt() thành long:
datetime             | production_type        | actual_generation_mw
2026-05-04 00:00:00  | Hydro Water Reservoir  | 1250.0
2026-05-04 00:00:00  | Wind Onshore           | 890.0
2026-05-04 00:00:00  | Nuclear                | 2100.0
Vấn đề nếu lưu wide format vào DuckDB: số columns thay đổi theo zone — SE1 có 5 production types, SE3 có 7. Không thể define schema cố định cho raw table. DuckDB cần schema cố định.
Đây là lý do kỹ thuật bắt buộc phải melt trước khi load — không phải vì muốn transform, mà vì wide format không load được vào relational table với fixed schema.
Ingestion layer được phép làm:        dbt làm:
─────────────────────────────────     ─────────────────────────
Shape để fit vào relational schema    Rename columns
(wide → long)                         Cast types
Fix column names (rename index)       Business logic
Drop fully null rows                  Joins
                                      Aggregations
                                      Derived metrics












Có 1 khái niệm then chốt trong kỹ thuật dữ liệu hiện đại: Sự tách biệt giữa nạp dữ liệu (Ingestion) và biến đổi dữ liệu (Transformation) trong mô hình ELT (Extract-Load-Transform) mà tôi muốn bạn xem xét cái nào hợp lý trong project này. 

Dưới đây là phân tích chi tiết về cách dữ liệu sẽ được xử lý hàng ngày:
1. Luồng hoạt động: Ingestion xong mới đến dbt
Trong kiến trúc ELT mà bạn đã thiết kế, quy trình không diễn ra đồng thời mà diễn ra theo trình tự được điều phối bởi Orchestrator Dagster

Bước 1 (Extract & Load): Dagster kích hoạt Python script để lấy dữ liệu từ ENTSO-E API và nạp thẳng vào DuckDB ở dạng thô (tầng Raw/Bronze). Lúc này, đúng như bạn nói, dữ liệu chưa được biến đổi. 
Bước 2 (Transform): Sau khi bước 1 hoàn thành thành công, Dagster sẽ kích hoạt lệnh dbt run hoặc dbt build. Lúc này, dbt mới bắt đầu quét dữ liệu mới về trong DuckDB để thực hiện các logic tính toán (như OEE) và đẩy kết quả sang các tầng tiếp theo (Staging -> Intermediate -> Marts).
2. dbt xử lý dữ liệu hàng ngày như thế nào?
Việc dbt xử lý dữ liệu hàng ngày "nặng" hay "nhẹ" phụ thuộc vào cách bạn Materialization cho các model
Nếu dùng View cho Các model ở tầng Staging khi bạn mở dashboard, DuckDB sẽ thực hiện các phép biến đổi ngay tại thời điểm đó trên dữ liệu mới nhất. Cách này đảm bảo dữ liệu luôn "tươi". Nếu dùng Table, dbt sẽ xóa bảng cũ và tạo bảng mới chứa toàn bộ dữ liệu (cả cũ và mới) mỗi ngày. Đây gọi là "Full Refresh". Nếu dùng Incremental (Xử lý gia tăng): Đây là cách chuyên nghiệp nhất cho việc xử lý hàng ngày. dbt sẽ chỉ lấy phần dữ liệu ENTSO-E của "ngày hôm nay" để biến đổi và chèn thêm vào bảng kết quả đã có, thay vì xử lý lại toàn bộ lịch sử. Bạn có thể dùng hàm is_incremental() để lọc dữ liệu dựa trên cột thời gian.
3. Đảm bảo dữ liệu "mới fetch về" được transform ngay
Để đảm bảo dữ liệu vừa nạp về không bị "bỏ quên", bạn cần áp dụng các cơ chế sau:
- Sự phụ thuộc trong Orchestrator: Trong Dagster, bạn cần thiết lập để lệnh dbt chạy ngay sau khi script Ingestion kết thúc thành công mỗi ngày
- Kiểm tra độ tươi (Source Freshness): Bạn có thể thiết lập dbt để kiểm tra xem dữ liệu trong DuckDB có đúng là dữ liệu của ngày hôm nay không trước khi chạy biến đổi
. Nếu dữ liệu ENTSO-E chưa về, dbt sẽ cảnh báo hoặc dừng pipeline để tránh báo cáo sai
.
Tính lặp lại (Idempotency): Hệ thống được thiết kế để dù bạn có chạy lại pipeline nhiều lần trong ngày, kết quả cuối cùng vẫn thống nhất và không bị trùng lặp dữ liệu
.
Tóm lại: Dữ liệu mới fetch về sẽ nằm ở trạng thái "thô" trong DuckDB một khoảng thời gian rất ngắn cho đến khi lệnh dbt run được kích hoạt bởi Dagster để hoàn tất quá trình biến đổi
. Quy trình này thường diễn ra tự động hoàn toàn theo lịch trình hàng ngày mà bạn đã thiết lập

