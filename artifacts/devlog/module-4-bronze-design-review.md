# Module 4 Bronze Design Review: Multi-source Raw Landing

## 1. Mục đích tài liệu

Tài liệu này đánh giá thiết kế Bronze trong `module-4-proposal.md`, cụ thể là ba bảng:

- `bronze.ingestion_batch`: lưu metadata của mỗi lần ingestion.
- `bronze.stock`: lưu stock records dưới dạng `JSONB` và liên kết về batch.
- `bronze.fund`: lưu fund/ETF records dưới dạng `JSONB` và liên kết về batch.

Mục tiêu là trả lời ba câu hỏi:

1. Thiết kế này có hợp lý cho bài toán multi-source của Module 4 hay không?
2. Thiết kế có dựa trên các practice và implementation có thật hay chỉ là schema tự nghĩ ra?
3. Những giới hạn và điều kiện nâng cấp của thiết kế là gì?

Phạm vi hiện tại gồm hai external providers:

- `vnstock`: danh sách thành phần VN30 và dữ liệu OHLCV cổ phiếu.
- SSI iBoard: dữ liệu lịch sử ETF `E1VFVN30`.

Ngày khảo sát: **2026-09-19**.

---

## 2. Kết luận ngắn

**Thiết kế Bronze hợp lý cho Module 4 là một shared Bronze schema với các dataset-aligned raw tables riêng biệt, không phải một generic record table và không phải một bảng đã join.**

Nó không phải một pattern tự chế. Hai ý tưởng cốt lõi của schema đều xuất hiện trong các ingestion framework và kiến trúc dữ liệu phổ biến:

1. Lưu payload gần với source dưới dạng JSON/semi-structured data, kèm ingestion metadata.
2. Có một thực thể load/batch riêng và gắn record với load/batch đó để audit, kiểm tra completion và reprocess.

Các implementation tương ứng:

- Airbyte từng dùng raw table có `_airbyte_data JSONB`, record id và extracted timestamp.
- dlt dùng `_dlt_loads` để theo dõi từng load package và `_dlt_load_id` trên data record.
- Databricks định nghĩa Bronze là raw, append-oriented, giữ lịch sử và thêm provenance metadata.
- dbt khuyến nghị staging bám theo từng source, không join hoặc aggregate ở staging.

Tuy nhiên, cần diễn đạt chính xác:

> `ingestion_batch + source-aligned raw tables` là cách triển khai của dự án dựa trên các pattern đã được sử dụng rộng rãi. Tên bảng và tập column cụ thể là quyết định của dự án, không phải một standard bắt buộc.

`bronze.stock` và `bronze.fund` dùng chung operational conventions nhưng không dùng chung payload contract. Cách tổ chức này bám sát pattern per-stream/per-resource của Airbyte và dlt, đồng thời tạo quan hệ một-một rõ ràng với các dbt staging models.

---

## 3. Thiết kế hiện tại đang giải quyết điều gì?

Ba bảng tách batch metadata khỏi hai dataset raw độc lập:

```text
                              ┌──── N  bronze.stock
bronze.ingestion_batch  1 ────┤        stock payload JSONB
                              │
                              └──── N  bronze.fund
                                       fund payload JSONB
```

### `ingestion_batch`

Đại diện cho một lần extract/load có thể audit được:

- Source/provider nào được gọi.
- Dataset nào được lấy.
- Partition hoặc khoảng thời gian nào được yêu cầu.
- Dagster run nào tạo batch.
- Raw file nằm ở đâu.
- Payload có checksum gì.
- Batch thành công, thất bại hay chỉ thành công một phần.
- Có bao nhiêu record được nhận và ghi.

### `stock` và `fund`

Mỗi bảng đại diện cho logical records của đúng một dataset family:

- Liên kết đến batch đã tạo record.
- Giữ source record key nếu có.
- Giữ event time/trade date ở dạng metadata kỹ thuật.
- Giữ payload gần nguyên bản bằng `JSONB`.
- Giữ checksum để hỗ trợ deduplication và upstream correction.

Sự phân tách này quan trọng vì một API response có thể chứa nhiều record. Nếu chỉ lưu từng row mà không lưu batch, hệ thống khó trả lời:

- Các row này đến từ request nào?
- Request đó có hoàn tất không?
- Có raw response gốc không?
- Rerun nào đã tạo duplicate?
- Batch nào bị thiếu record?

---

## 4. Evidence từ các implementation và practice có thật

### 4.1 Airbyte: raw event payload + ingestion metadata

Airbyte Postgres destination từng tạo raw table với các field:

- `_airbyte_raw_id`: ID của raw event.
- `_airbyte_extracted_at`: thời điểm record được lấy từ source.
- `_airbyte_loaded_at`: thời điểm raw row được xử lý vào final table.
- `_airbyte_data`: source event dưới dạng `JSONB`.

Đây là bằng chứng trực tiếp cho pattern “record metadata + raw JSON payload” mà `bronze.stock` và `bronze.fund` đang áp dụng.

Airbyte hiện ghi rõ raw-table mode này đã deprecated từ connector Postgres 3.0 để chuyển sang Direct Load. Vì vậy không nên nói rằng raw table vẫn là kiến trúc mới nhất của Airbyte Postgres. Dù vậy, implementation của nó vẫn là một precedent rõ ràng và đã được sử dụng thực tế cho raw landing.

Nguồn:

- [Airbyte repository: Postgres destination documentation](https://github.com/airbytehq/airbyte/blob/master/docs/integrations/destinations/postgres.md)
- [Airbyte repository: S3 destination raw record format](https://github.com/airbytehq/airbyte/blob/master/docs/integrations/destinations/s3.md)
- [Airbyte repository](https://github.com/airbytehq/airbyte)

Mapping sang thiết kế dự án:

| Airbyte | Module 4 |
|---|---|
| `_airbyte_raw_id` | `stock_record_id` / `fund_record_id` |
| `_airbyte_extracted_at` | `ingested_at` |
| `_airbyte_data JSONB` | `payload JSONB` |
| `_airbyte_meta` / sync metadata | `ingestion_batch` + record metadata |

Airbyte legacy raw mode tạo raw table theo stream. Module 4 áp dụng cùng nguyên tắc bằng cách tách `bronze.stock` và `bronze.fund`, trong khi vẫn dùng chung `ingestion_batch` cho operational lineage.

### 4.2 dlt: load package + load history + record-to-load lineage

dlt tạo một load package có ID riêng cho mỗi lần pipeline chạy. Trong destination:

- `_dlt_loads` lưu lịch sử load.
- Data tables mang `_dlt_load_id` để xác định load nào tạo record.
- `_dlt_version` lưu schema version.
- `_dlt_pipeline_state` lưu pipeline state và incremental cursor.

dlt cũng mô tả extract phase là bước lưu raw data nhận từ source vào một load package trên disk trước khi normalize và load.

Đây là precedent gần nhất cho quan hệ:

```text
ingestion_batch.batch_id ← stock.batch_id / fund.batch_id
```

Nguồn:

- [dlt docs: How dlt works](https://dlthub.com/docs/reference/explainers/how-dlt-works)
- [dlt docs: Destination tables and `_dlt_loads`](https://dlthub.com/docs/general-usage/destination-tables)
- [dlt repository: destination tables documentation](https://github.com/dlt-hub/dlt/blob/devel/docs/website/docs/general-usage/destination-tables.md)
- [dlt repository](https://github.com/dlt-hub/dlt)

Mapping sang thiết kế dự án:

| dlt | Module 4 |
|---|---|
| Load package | Raw response/file + ingestion operation |
| `_dlt_loads` | `bronze.ingestion_batch` |
| `_dlt_load_id` | `bronze.stock.batch_id` / `bronze.fund.batch_id` |
| Schema version hash | `schema_version`/payload contract version |
| Pipeline state | Dagster partition/cursor metadata |

dlt thường normalize dữ liệu thành table theo resource. Module 4 chủ động trì hoãn normalization business schema sang dbt vì mục tiêu của module là khám phá source trước khi chốt model.

### 4.3 Databricks Medallion: Bronze giữ raw history và provenance

Databricks mô tả Bronze với các đặc tính:

- Giữ dữ liệu ở trạng thái và format gần source.
- Append incremental và tăng dần theo thời gian.
- Là nguồn để xây dựng Silver, không phải dataset business cho analyst.
- Giữ lịch sử để reprocess và audit.
- Chỉ làm validation tối thiểu.
- Có thể thêm metadata về provenance, ví dụ tên source file.

Databricks cũng đặt cleanup, deduplication, type casting, schema enforcement và join ở Silver thay vì Bronze.

Nguồn:

- [Databricks: Medallion architecture](https://docs.databricks.com/aws/en/lakehouse/medallion)
- [Databricks: Reliability best practices](https://docs.databricks.com/aws/en/lakehouse-architecture/reliability/best-practices)

Điều này hỗ trợ trực tiếp các quyết định:

- Bronze lưu raw JSON và original files.
- Bronze chỉ parse tối thiểu để tách logical record.
- Không join stock và ETF trong Bronze.
- Deduplicate business record và normalize type bằng dbt downstream.

### 4.4 dbt: staging theo source, không join sớm

dbt Labs khuyến nghị:

- Tổ chức staging subdirectory theo source system.
- Một staging model tương ứng một source table.
- Staging dùng cho rename, type casting và basic computations.
- Tránh join và aggregation trong staging vì chúng thay đổi grain và làm quan hệ downstream khó hiểu.

Nguồn:

- [dbt Labs: Staging—Preparing atomic building blocks](https://docs.getdbt.com/best-practices/how-we-structure/2-staging)

Áp dụng vào Module 4:

```text
bronze.stock ─────────── stg_vnstock__stock_daily
bronze.fund ──────────── stg_ssi__e1vfvn30_daily

staging models
    └── exploration.exp_vn30_vs_e1vfvn30_daily
```

Join multi-source nằm trong model `exploration` hoặc một intermediate model, không nằm trong Bronze và cũng không nên nằm trong source-conformed staging model.

### 4.5 PostgreSQL JSONB: lựa chọn storage có hỗ trợ chính thức

PostgreSQL phân biệt:

- `json`: giữ exact input text và phải parse lại khi query.
- `jsonb`: lưu dạng binary decomposition, input chậm hơn một chút nhưng xử lý nhanh hơn và hỗ trợ indexing.

Vì Bronze cần dbt đọc field trong payload, `JSONB` hợp lý hơn `JSON`. Tuy nhiên, raw response/file nguyên bản vẫn phải được lưu riêng nếu cần byte-for-byte fidelity, vì `JSONB` không bảo toàn whitespace, key order hoặc duplicate keys như raw text.

Nguồn:

- [PostgreSQL documentation: JSON types and JSONB indexing](https://www.postgresql.org/docs/current/datatype-json.html)

---

## 5. So sánh side-by-side

| Đặc tính | Module 4 | Airbyte legacy raw | dlt | Databricks Bronze |
|---|---|---|---|---|
| Raw/semi-structured payload | `payload JSONB` | `_airbyte_data JSONB` | Raw load package; JSON supported | String/VARIANT/binary/raw format |
| Record metadata | ID, event time, checksum | raw ID, extracted/loaded time, meta | `_dlt_id`, `_dlt_load_id` | Provenance metadata |
| Load/batch metadata | `ingestion_batch` | Sync metadata | `_dlt_loads` | Job/file metadata tùy implementation |
| Record liên kết về load | `batch_id` FK | Sync metadata | `_dlt_load_id` | Thường bằng ingestion metadata |
| Giữ original file/response | Có | Tùy destination | Có load package trong extract phase | Có/được khuyến nghị ở raw storage |
| Business cleanup trong raw | Không | Không hoặc normalization downstream | Normalize phase riêng | Không |
| Join trong raw | Không | Không | Không | Không; join ở Silver |
| Physical layout | Một table cho stock, một table cho fund | Thường per stream | Per resource/table | Thường per source/dataset |

Kết luận từ bảng trên:

- Hai primitive quan trọng nhất của Module 4—raw payload và load lineage—đều có precedent rõ ràng.
- Module 4 bám theo physical layout per dataset family thay vì gom mọi payload vào một bảng chung.
- Điểm dùng chung nằm ở batch metadata và operational columns, không nằm ở business payload hoặc grain.

---

## 6. Decision matrix

Điểm 1–5, càng cao càng tốt.

| Tiêu chí | Một bảng business-typed cho mỗi source | Một generic `raw_record` | Dataset-aligned raw tables + staging |
|---|---:|---:|---:|
| Thêm source mới nhanh | 2 | 5 | 4 |
| Chịu schema drift | 2 | 5 | 5 |
| Audit/reprocess | 3 | 5 | 5 |
| Query trực tiếp | 5 | 2 | 4 |
| dbt usability | 5 | 2 | 5 |
| Hiệu năng khi volume lớn | 5 | 2 | 4 |
| Đơn giản cho Module 4 | 3 | 5 | 4 |
| Tránh data swamp | 4 | 2 | 5 |
| Tổng | 29/40 | 28/40 | **36/40** |

Khuyến nghị là phương án dataset-aligned raw tables + staging:

- Shared operational contract cho các bảng Bronze, nhưng physical table và payload contract riêng cho stock và fund.
- Source-specific staging trong dbt.
- Original file/response được giữ ngoài database.
- Có index, retention và quyền truy cập độc lập cho từng dataset.

---

## 7. Schema contract được khuyến nghị

Đây là logical contract, chưa phải migration SQL cuối cùng.

### 7.1 `bronze.ingestion_batch`

| Column | Vai trò |
|---|---|
| `batch_id` | UUID định danh duy nhất của batch |
| `source_name` | Logical source, ví dụ `vnstock`, `ssi_iboard` |
| `dataset_name` | Stream/dataset, ví dụ `stock_daily`, `e1vfvn30_daily` |
| `provider_name` | Provider thực tế, ví dụ `KBS`, `VCI`, `SSI` |
| `partition_key` | Dagster partition hoặc logical extraction partition |
| `requested_from` | Đầu khoảng thời gian request |
| `requested_to` | Cuối khoảng thời gian request |
| `request_metadata` | URL/params an toàn; không lưu secret |
| `dagster_run_id` | Liên kết về orchestration run |
| `raw_object_path` | Đường dẫn raw response/file |
| `payload_checksum` | Checksum của raw object |
| `schema_version` | Phiên bản source contract/parser |
| `started_at` | Thời điểm bắt đầu |
| `finished_at` | Thời điểm kết thúc |
| `status` | `started`, `completed`, `failed`, `partial` |
| `record_count` | Số record nhận được |
| `error_message` | Error summary nếu batch thất bại |

### 7.2 Shared record columns cho `bronze.stock` và `bronze.fund`

| Column | Vai trò |
|---|---|
| `stock_record_id` / `fund_record_id` | Surrogate key của raw row trong từng table |
| `batch_id` | FK về `ingestion_batch` |
| `source_record_key` | Natural/deterministic key nếu xác định được |
| `event_time` | Timestamp từ source, chưa áp business timezone |
| `trade_date` | Technical projection để pruning/join downstream |
| `payload` | Record gần nguyên bản dưới dạng `JSONB` |
| `payload_checksum` | Hash ổn định của canonical payload |
| `ingested_at` | Thời điểm database nhận record |

### 7.3 Metadata nào được phép tách khỏi JSONB?

Việc đưa các field sau ra column riêng không làm Bronze mất tính raw:

- `batch_id`
- `source_record_key`
- `event_time`
- `trade_date`
- `ingested_at`
- `payload_checksum`

Đây là operational metadata phục vụ lineage, partition pruning và idempotency, không phải business transformation.

Không nên đưa các field derived như sau vào Bronze:

- daily return
- volatility
- market-cap estimate
- ETF tracking difference
- aggregated VN30 return
- normalized cross-source symbol mapping

Những field đó thuộc staging/intermediate/exploration.

---

## 8. Idempotency và source correction

Không nên dùng `INSERT OR REPLACE` cho raw Bronze vì nó phá lịch sử correction.

Khuyến nghị:

1. Raw batch/file là immutable.
2. Cùng raw payload và cùng extraction identity không được insert lại.
3. Nếu source sửa record cũ và payload checksum thay đổi, lưu version mới.
4. dbt staging chọn version mới nhất cho business key.

Hai loại hash cần phân biệt:

- `ingestion_batch.payload_checksum`: hash toàn response/file.
- `stock.payload_checksum` / `fund.payload_checksum`: hash của một logical record sau canonical JSON serialization.

Khóa chống duplicate không nên chỉ là `batch_id + payload_checksum`, vì batch mới có thể lấy lại đúng record cũ. Tùy mục tiêu lưu lịch sử, có hai policy:

### Policy A — giữ mọi observation

Unique trong batch:

```text
(batch_id, source_record_key, payload_checksum)
```

Cùng record có thể xuất hiện ở batch khác. Phù hợp khi cần audit mỗi lần quan sát source.

### Policy B — chỉ giữ mỗi distinct source version

Unique xuyên batch:

```text
(source_name, dataset_name, source_record_key, payload_checksum)
```

Tiết kiệm storage nhưng mất thông tin “record này được nhìn thấy trong batch nào” nếu không có bridge table.

**Khuyến nghị cho Module 4: Policy A.** Dataset nhỏ, auditability có giá trị hơn tiết kiệm vài MB. dbt staging chịu trách nhiệm deduplicate thành latest logical version.

---

## 9. Áp dụng cụ thể cho hai nguồn

### 9.1 vnstock stock daily

Một batch có thể đại diện cho:

```text
source_name    = vnstock
dataset_name   = stock_daily
provider_name  = KBS hoặc VCI
partition_key  = YYYY-MM-DD
```

Một record:

```text
source_record_key = <symbol>:<trade_date>:1D
event_time        = timestamp/date trả từ provider
payload           = row OHLCV gần nguyên bản
```

Không nên ghi chung provider là `vnstock` rồi bỏ mất KBS/VCI. `vnstock` là client/abstraction layer; provider thực tế vẫn là lineage quan trọng.

### 9.2 SSI E1VFVN30 daily

SSI history API trả các mảng song song `t`, `o`, `h`, `l`, `c`, `v`.

Quy trình Bronze hợp lý:

1. Lưu toàn response JSON vào raw storage.
2. Hash toàn response và tạo `ingestion_batch`.
3. Kiểm tra các mảng bắt buộc có cùng chiều dài.
4. Chuyển mỗi array index thành một logical record mà không đổi ý nghĩa giá trị.
5. Lưu logical record vào `bronze.fund.payload`.

Việc zip các parallel arrays thành object per candle là technical parsing cần thiết để có record boundary; nó chưa phải business normalization.

Ví dụ key:

```text
source_record_key = E1VFVN30:<trade_date>:D
```

---

## 10. Khi nào cần partition từng bảng Bronze?

Việc tách `bronze.stock` và `bronze.fund` đã cô lập index, vacuum, retention và access pattern giữa hai dataset. Bước tiếp theo chỉ là cân nhắc partition bên trong từng bảng khi volume thực tế đủ lớn.

### Chưa cần partition khi

- Ingestion là daily batch và tổng record còn nhỏ.
- Index của từng table vẫn nhỏ và query plan ổn định.
- Retention chưa cần bulk detach/drop theo thời gian.
- PostgreSQL chưa có dấu hiệu scan/index/vacuum bottleneck.

### Nên partition một table khi

- Table tăng đến mức không còn fit hợp lý trong memory/index strategy.
- Có tần suất intraday cao hoặc lịch sử dài.
- Query chủ yếu filter theo `trade_date`/`ingested_at`.
- Bulk retention/delete theo tháng hoặc năm trở thành nhu cầu thường xuyên.

PostgreSQL khuyến nghị chỉ partition khi table đủ lớn để lợi ích pruning và maintenance vượt chi phí quản lý partition. Không nên tạo partition sớm chỉ để “trông enterprise”.

Nguồn:

- [PostgreSQL documentation: Table partitioning](https://www.postgresql.org/docs/current/ddl-partitioning.html)

Nếu cần nâng cấp, thứ tự hợp lý là:

1. Index relational metadata của từng table theo access pattern thực tế.
2. Tạo source-specific staging views/models.
3. Partition `bronze.stock` hoặc `bronze.fund` theo `trade_date`/`ingested_at` theo tháng nếu retention và time pruning là chính.

Không cần partition ngay khi init chỉ để “trông enterprise”.

---

## 11. Index strategy tối thiểu

Không cần GIN index toàn bộ `payload` mặc định. GIN có write/storage cost và chỉ có ích khi query JSON keys bất định trực tiếp trong Bronze.

Các index ưu tiên:

```text
ingestion_batch(source_name, dataset_name, partition_key)
ingestion_batch(dagster_run_id)
ingestion_batch(payload_checksum)
stock(batch_id)
stock(source_record_key, trade_date)
stock(payload_checksum)
fund(batch_id)
fund(source_record_key, trade_date)
fund(payload_checksum)
```

dbt staging đọc trực tiếp đúng source-aligned table, filter bằng relational metadata trước, sau đó mới extract field từ JSONB.

Chỉ thêm expression index hoặc GIN khi có query profile chứng minh cần thiết.

---

## 12. Raw fidelity: PostgreSQL không thay thế object/file storage

`JSONB` không phải byte-for-byte archive của response. Nó có thể không giữ:

- Whitespace.
- Thứ tự key.
- Duplicate JSON keys.
- HTTP headers.
- Exact body bytes trước khi decode.

Vì vậy kiến trúc đúng là:

```text
Raw file/object  = bằng chứng nguồn gần nguyên bản nhất
PostgreSQL batch = catalog, lineage và trạng thái load
PostgreSQL JSONB = logical records để dbt có thể đọc
```

Đối với CSV, Excel, PDF hoặc HTML, raw file bắt buộc được giữ. Đối với JSON API, vẫn nên giữ response body nguyên bản ít nhất trong retention period của Module 4.

---

## 13. Data quality boundary

Bronze chỉ nên fail batch đối với lỗi làm dữ liệu không thể được lưu hoặc truy vết an toàn:

- HTTP transport thất bại sau retry.
- Body không đọc/parse được theo declared content type.
- SSI parallel arrays khác chiều dài.
- Không ghi được raw object.
- Checksum hoặc batch metadata không tạo được.
- Database transaction thất bại.

Bronze không nên loại record chỉ vì:

- Giá trị OHLC null.
- Volume bằng 0.
- Một ticker thiếu một ngày.
- `high < low` do source lỗi.
- Source đổi hoặc thêm field.

Các lỗi dữ liệu này nên được lưu raw rồi quarantine/cảnh báo trong dbt staging hoặc asset checks. Nhờ vậy pipeline không silently drop evidence của source error.

---

## 14. Join multi-source đặt ở đâu?

Theo Databricks và dbt practice:

- Bronze: raw ingestion và provenance.
- Staging: mỗi source được rename, cast và chuẩn hóa độc lập.
- Exploration/intermediate: join stock và ETF.

Flow cuối cùng:

```text
bronze.ingestion_batch
        ├── bronze.stock ── stg_vnstock__stock_daily ──┐
        └── bronze.fund  ── stg_ssi__e1vfvn30_daily ──┤
                                                      └── exp_vn30_vs_e1vfvn30_daily
```

Join key chính cho EDA là `trade_date`, sau khi hai source đã được chuẩn hóa timezone và deduplicate.

Không đưa join result trở lại `bronze`.

---

## 15. So sánh với repo `iboard-etl`

Repo tham khảo `quangnt03/iboard-etl` dùng:

- Pydantic để parse SSI response.
- Một typed SQLite table `vn30_stock`.
- Unique `(ticker, timestamp)`.
- `INSERT OR REPLACE` để rerun.
- SQL analytics trực tiếp trên typed table.
- `vnstock` làm fallback cho dữ liệu history.

Nguồn:

- [iboard-etl repository](https://github.com/quangnt03/iboard-etl)
- [HTTP ingestion](https://github.com/quangnt03/iboard-etl/blob/main/src/ingest.py)
- [VN30 Pydantic models](https://github.com/quangnt03/iboard-etl/blob/main/src/models/vn30_stock.py)
- [SQLite repository](https://github.com/quangnt03/iboard-etl/blob/main/src/repository/vn30_repository.py)
- [vnstock history wrapper](https://github.com/quangnt03/iboard-etl/blob/main/tools/vnstock_history.py)

| Tiêu chí | `iboard-etl` | Module 4 Bronze |
|---|---|---|
| Scope | Một source/dataset chính | Multi-source |
| Storage | Typed SQLite table | Shared batch metadata + separate stock/fund JSONB tables |
| Schema drift | Phải sửa Pydantic/SQL table | Có thể giữ payload mới trước khi staging cập nhật |
| Audit raw response | Hạn chế | Có raw object + checksum |
| Source correction | Replace row | Giữ version mới theo batch |
| dbt integration | Chưa có | Thiết kế làm input cho dbt |
| Phù hợp EDA source chưa ổn định | Trung bình | Cao |

Vì Module 4 đang ở giai đoạn khám phá hai source và chưa chốt business schema, dataset-aligned Bronze tables với raw JSONB phù hợp hơn typed business table của `iboard-etl`. Khi source contract đã ổn định, typed staging models sẽ lấy lại query ergonomics mà không làm mất raw history.

---

## 16. Các quyết định chính thức cho Module 4

1. Giữ một shared `ingestion_batch` và hai raw tables độc lập: `bronze.stock`, `bronze.fund`.
2. Mỗi raw table có `payload JSONB` và source-specific payload contract riêng.
3. Lưu original response/file trong `storage/raw` hoặc object storage tương đương.
4. Bronze append-oriented; không `INSERT OR REPLACE` raw history.
5. Ghi cả logical source (`vnstock`) và actual provider (`KBS`/`VCI`).
6. Dùng `batch_id` để liên kết mỗi record với Dagster run và raw object.
7. Parse source ở mức tối thiểu để xác định record boundary.
8. Không business join stock và ETF trong Bronze.
9. dbt staging được tổ chức theo source.
10. Deduplication business key, type casting và data-quality enforcement nằm downstream.
11. Chưa partition `stock` hoặc `fund` ở thời điểm init; thêm partition riêng cho table nào có số liệu vận hành chứng minh cần.
12. Chưa thêm GIN index toàn payload; chỉ thêm khi có query profile cụ thể.

---

## 17. Những claim không nên đưa vào báo cáo

Không nên nói:

- “Đây là schema chuẩn chính thức của Medallion.”
- “Airbyte hiện vẫn luôn dùng raw JSONB table cho Postgres.”
- “Multi-source nghĩa là join hoặc union các source trong Bronze.”
- “JSONB là bản sao nguyên byte của source response.”
- “Deduplication phải xảy ra trong Bronze.”
- “Join nhiều source trong Bronze là consolidation.”

Nên nói:

> Thiết kế sử dụng các pattern đã được chứng minh bởi Airbyte, dlt và Medallion architecture: giữ raw payload, thêm ingestion metadata, theo dõi load/batch lineage và trì hoãn business transformations. Dự án dùng một shared Bronze schema nhưng tách physical raw table theo dataset family (`stock`, `fund`). Hai bảng chỉ dùng chung operational contract, không dùng chung payload schema và không được join trong Bronze.

---

## 18. Final recommendation

Giữ thiết kế Bronze theo mô hình dataset-aligned tables và bảo đảm proposal/migration bao gồm rõ:

- `source_name` và `dataset_name`.
- `provider_name`.
- `batch status`.
- `raw_object_path`.
- Batch-level và record-level checksum.
- `schema_version`.
- `source_record_key`, `event_time`, `trade_date`.
- Append/versioning semantics.

Với các bổ sung đó, thiết kế vừa đủ đơn giản cho ba tuần của Module 4, vừa có lineage và reprocessing tốt, đồng thời không khóa dự án vào schema business được quyết định quá sớm.

Lý do chọn raw dataset-aligned tables thay vì business-typed tables ngay từ đầu:

- Hai source có response shape khác nhau.
- SSI endpoint không có public contract/SLA ổn định.
- Mục tiêu của module là EDA và đánh giá compatibility trước khi chốt model.
- Raw retention cho phép sửa parser/dbt mà không gọi lại nguồn.
- dbt vẫn tạo được typed, source-specific interface cho downstream.

Đây là một quyết định kiến trúc có precedent, có trade-off được ghi rõ và có migration path; không phải một schema được tạo tùy ý.
