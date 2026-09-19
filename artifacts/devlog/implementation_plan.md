# Implementation Plan: Phase 1 — Self-hosted Dagster on VPS

## 1. Mục tiêu

Phase 1 tập trung duy nhất vào việc khởi tạo repository như một Dagster application có thể được containerize và vận hành ổn định trên VPS cá nhân.

Kết quả cần đạt:

- Dagster webserver hoạt động trên VPS.
- Dagster daemon hoạt động và có thể xử lý schedules/sensors trong các phase sau.
- Dagster code location của repository được load thành công.
- Dagster metadata được lưu bền vững trong PostgreSQL.
- Có một healthcheck asset tối thiểu để xác minh end-to-end deployment.
- Restart hoặc recreate application containers không làm mất run history.
- Repository sẵn sàng để bổ sung stock ingestion, fund ingestion và dbt ở các phase tiếp theo.

Phase này chưa triển khai data pipeline thật.

---

## 2. Phạm vi Phase 1

### Trong phạm vi

- Khởi tạo Git repository và Python project.
- Thiết lập `src` layout cho Python package.
- Cài đặt các dependency tối thiểu của Dagster.
- Tạo Dagster `Definitions` entry point.
- Tạo một healthcheck asset không phụ thuộc external API.
- Tạo Docker image dùng chung cho Dagster services và code location.
- Tạo Docker Compose stack cho VPS.
- Thiết lập PostgreSQL làm Dagster instance storage.
- Thiết lập Dagster webserver, daemon và gRPC code server.
- Thiết lập environment variables và persistent volumes.
- Thêm các script deploy, log và smoke test tối thiểu.
- Viết hướng dẫn local development và VPS deployment.
- Thêm automated test để kiểm tra `Definitions` có thể load.

### Ngoài phạm vi

- Chưa tạo stock hoặc fund assets thật.
- Chưa tạo Bronze database hoặc Bronze migrations.
- Chưa triển khai dbt hoặc `dagster-dbt`.
- Chưa tạo dbt models, tests, profiles hoặc packages.
- Chưa tạo EDA notebook.
- Chưa cấu hình domain, Nginx, TLS hoặc public Internet access.
- Chưa triển khai CI/CD tự động.
- Chưa triển khai monitoring ngoài khả năng quan sát mặc định của Dagster và Docker.
- Chưa triển khai PostgreSQL backup automation.

Các khu vực chưa được triển khai chỉ có `.gitkeep` để giữ vị trí trong repository.

### Phần mở rộng đã được duyệt: standalone source probes

Phase 1 được phép bổ sung hai source adapter chạy độc lập để xác minh upstream trước khi thiết kế Dagster assets và Bronze persistence:

- `vnstock` daily stock OHLCV probe.
- SSI `E1VFVN30` daily fund/ETF history probe.
- JSON raw output nằm dưới `storage/raw/` và không được commit.
- Source logic nằm trong `src/data_pipeline/ingestion/` và không import Dagster.
- `scripts/` chỉ chứa CLI wrappers; Dagster assets tương lai gọi lại cùng source functions.

Phần mở rộng này chưa bao gồm schedules, partitions, database loading hoặc Bronze migrations.

---

## 3. Kiến trúc deployment

```text
Developer machine
    │
    │ git push / pull hoặc deployment script
    ▼
Personal VPS
    │
    ├── dagster-webserver
    │       └── Dagster UI và GraphQL API
    │
    ├── dagster-daemon
    │       └── daemon, schedule và sensor runtime
    │
    ├── dagster-code
    │       └── gRPC code server
    │           └── data_pipeline.definitions
    │
    └── postgres
            └── Dagster run/event/schedule metadata
```

Ba Dagster services sử dụng cùng một application image nhưng chạy các command khác nhau.

```text
dagster-webserver → dagster-webserver
dagster-daemon    → dagster-daemon run
dagster-code      → dagster api grpc
```

`dagster-webserver` và `dagster-daemon` không import business code trực tiếp. Chúng đọc `workspace.yaml` và kết nối tới `dagster-code` qua Docker network.

---

## 4. Cây codebase Phase 1

```text
my-distributed-data-pipeline/
├── README.md
├── AGENTS.md
├── module-4-proposal.md
├── module-4-bronze-design-review.md
├── implementation_plan.md
│
├── pyproject.toml
├── uv.lock
├── .python-version
├── .env.example
├── .gitignore
├── compose.yml
│
├── src/
│   └── data_pipeline/
│       ├── __init__.py
│       ├── definitions.py
│       │
│       ├── assets/
│       │   ├── __init__.py
│       │   └── healthcheck.py
│       │
│       ├── resources/
│       │   └── __init__.py
│       │
│       ├── ingestion/
│       │   ├── stock/
│       │   │   └── .gitkeep
│       │   └── fund/
│       │       └── .gitkeep
│       │
│       └── dbt/
│           └── .gitkeep
│
├── migrations/
│   └── .gitkeep
│
├── docker/
│   ├── Dockerfile
│   ├── entrypoint.sh
│   │
│   └── dagster/
│       ├── dagster.yaml
│       └── workspace.yaml
│
├── notebooks/
│   └── .gitkeep
│
├── tests/
│   ├── conftest.py
│   └── integration/
│       └── test_definitions.py
│
├── scripts/
│   ├── deploy.sh
│   ├── logs.sh
│   └── smoke-test.sh
│
├── storage/
│   ├── raw/
│   │   └── .gitkeep
│   ├── exports/
│   │   └── .gitkeep
│   └── backups/
│       └── .gitkeep
│
└── docs/
    ├── architecture.md
    ├── local-development.md
    └── vps-deployment.md
```

Không tạo các Python module stock/fund rỗng trong Phase 1. Chỉ tạo package thực sự cần để Dagster load được và dùng `.gitkeep` cho các khu vực tương lai.

---

## 5. Python project

### Package

Tên import package:

```text
data_pipeline
```

Dagster entry point:

```text
data_pipeline.definitions:defs
```

### Dependency tối thiểu

Phase 1 chỉ cần nhóm dependency phục vụ Dagster deployment:

- `dagster`
- `dagster-webserver`
- `dagster-postgres`
- PostgreSQL driver tương thích với version được pin

Development dependencies:

- `pytest`
- Formatter/linter chỉ thêm nếu được chốt trong lúc implementation.

Chưa thêm:

- `dbt-core`
- `dbt-postgres`
- `dagster-dbt`
- `vnstock`
- `pandas`
- HTTP client dành cho SSI

Tất cả production dependencies phải được pin thông qua `uv.lock` để local và VPS sử dụng cùng dependency graph.

---

## 6. Dagster code location

### `definitions.py`

`definitions.py` là entry point duy nhất của code location.

Trong Phase 1, file này chỉ đăng ký:

- Healthcheck asset.
- Không có schedule.
- Không có sensor.
- Không có production job tùy chỉnh.
- Không có dbt assets.

### Healthcheck asset

Healthcheck asset phải:

- Không gọi Internet.
- Không phụ thuộc PostgreSQL pipeline database.
- Không ghi dữ liệu business.
- Trả về metadata đơn giản như application version và timestamp.
- Có thể materialize từ Dagster UI.

Mục đích của asset là kiểm tra đầy đủ flow:

```text
Dagster UI
    → launch run
    → Dagster run storage
    → code server
    → execute Python asset
    → event log storage
    → result hiển thị lại trên UI
```

---

## 7. Docker image

### Yêu cầu

Một production Docker image dùng chung cho:

- `dagster-webserver`
- `dagster-daemon`
- `dagster-code`

Image cần:

1. Sử dụng Python version được pin trong `.python-version`.
2. Cài dependencies từ lockfile.
3. Cài Python package từ repository.
4. Copy `src/` và Dagster configuration cần thiết.
5. Chạy bằng non-root user nếu không làm phức tạp volume permissions quá mức.
6. Không bake `.env` hoặc credentials vào image.
7. Có healthcheck phù hợp cho code server hoặc service tương ứng.

### Build context

`compose.yml` nằm ở repository root và build từ root để Docker truy cập được:

- `pyproject.toml`
- `uv.lock`
- `src/`
- `docker/`

### Entrypoint

`entrypoint.sh` chỉ xử lý các bước runtime dùng chung thực sự cần thiết. Không đặt database migration hoặc business initialization vào entrypoint trong Phase 1.

---

## 8. Docker Compose services

### `postgres`

Vai trò:

- Lưu Dagster run storage.
- Lưu event log storage.
- Lưu schedule storage.

Yêu cầu:

- Dùng named volume cho PostgreSQL data directory.
- Có healthcheck bằng `pg_isready`.
- Không expose cổng PostgreSQL ra public interface trên VPS nếu không cần.
- Credentials lấy từ `.env`.

Phase 1 chỉ sử dụng database cho Dagster metadata. Pipeline database/schema sẽ được bổ sung khi triển khai Bronze.

### `dagster-code`

Vai trò:

- Chạy Dagster gRPC code server.
- Load `data_pipeline.definitions`.
- Expose gRPC port trong internal Docker network.

Yêu cầu:

- Chỉ webserver và daemon cần truy cập service này.
- Có restart policy.
- Phụ thuộc PostgreSQL health chỉ khi runtime thực sự yêu cầu; code server không nên phụ thuộc cứng vào pipeline database.

### `dagster-webserver`

Vai trò:

- Cung cấp Dagster UI.
- Đọc workspace configuration.
- Kết nối Dagster metadata PostgreSQL.
- Kết nối `dagster-code` qua gRPC.

Trong Phase 1, có thể expose Dagster port trực tiếp để smoke test trên VPS. Public Internet exposure, reverse proxy và TLS nằm ngoài phạm vi.

Khuyến nghị ban đầu:

- Bind vào VPS/private firewall rule có kiểm soát.
- Không mở Dagster UI cho toàn Internet mà không có authentication layer.

### `dagster-daemon`

Vai trò:

- Chạy Dagster daemon process.
- Chuẩn bị runtime cho schedules/sensors trong phase sau.

Dù Phase 1 chưa có schedule, daemon vẫn được deploy để xác minh topology production ngay từ đầu.

---

## 9. Dagster instance configuration

### `dagster.yaml`

Cấu hình:

- PostgreSQL run storage.
- PostgreSQL event log storage.
- PostgreSQL schedule storage.
- Compute logs ở persistent volume hoặc một mounted host directory.
- Run launcher mặc định trong Phase 1, trừ khi Docker run launcher được chọn riêng sau này.

Secrets phải được resolve từ environment variables, không ghi thẳng vào YAML.

### `workspace.yaml`

Workspace chỉ khai báo một gRPC code location:

```text
host: dagster-code
port: <internal-grpc-port>
location_name: data_pipeline
```

Không dùng local Python file target trong production Compose vì webserver/daemon nên giao tiếp với code location độc lập.

---

## 10. Environment variables

`.env.example` chỉ chứa tên biến và giá trị mẫu không nhạy cảm.

Nhóm biến tối thiểu:

```text
POSTGRES_USER
POSTGRES_PASSWORD
POSTGRES_DB
POSTGRES_HOST
POSTGRES_PORT

DAGSTER_HOME
DAGSTER_WEBSERVER_PORT
DAGSTER_CODE_PORT
```

Nguyên tắc:

- `.env` thật bị ignore.
- Không commit password hoặc hostname riêng của VPS.
- Docker services dùng service name `postgres` và `dagster-code` trong internal network.
- Không dùng `localhost` để kết nối giữa containers.

---

## 11. Persistent storage

Phase 1 cần persistence cho:

- PostgreSQL data.
- Dagster compute logs nếu cấu hình lưu trên filesystem.

Named volumes dự kiến:

```text
dagster_postgres_data
dagster_compute_logs
```

Các thư mục sau chỉ là placeholder cho phase sau:

```text
storage/raw/
storage/exports/
storage/backups/
```

Không mount `storage/raw` vào application container cho tới khi ingestion được triển khai.

---

## 12. Scripts vận hành

### `scripts/deploy.sh`

Trách nhiệm dự kiến:

1. Validate `.env` tồn tại.
2. Pull source hoặc được gọi sau khi source đã được cập nhật.
3. Build application image.
4. Start/update Compose services.
5. Chờ service healthchecks.
6. Gọi smoke test.

Script không tự ý reset database hoặc xóa volume.

### `scripts/logs.sh`

Hiển thị log của:

- `dagster-webserver`
- `dagster-daemon`
- `dagster-code`
- `postgres`

### `scripts/smoke-test.sh`

Kiểm tra tối thiểu:

- Compose services đang chạy.
- PostgreSQL healthy.
- Dagster UI/GraphQL endpoint phản hồi.
- Code location load thành công.
- Không có repository location load error.

Materialization healthcheck asset có thể được thực hiện thủ công trong UI ở bước đầu hoặc tự động qua Dagster GraphQL nếu việc đó không làm implementation phức tạp quá mức.

---

## 13. Testing

### Automated test

`tests/integration/test_definitions.py` cần kiểm tra:

- Import `data_pipeline.definitions` thành công.
- `defs` là Dagster `Definitions` hợp lệ.
- Healthcheck asset được đăng ký.
- Definitions validation không phát hiện duplicate hoặc unresolved dependency.

Test này chạy không cần Docker và không cần external services.

### Container verification

Sau khi build:

- Application image import được package.
- gRPC code server start được.
- Webserver nhìn thấy code location.
- Daemon kết nối được Dagster metadata storage.

### VPS verification

- UI truy cập được từ địa chỉ được cho phép.
- Healthcheck asset materialize thành công.
- Run xuất hiện trong run history.
- Restart webserver, daemon và code server không mất history.
- Recreate application containers không mất history.
- Restart PostgreSQL container không mất data volume.

---

## 14. Documentation

### `README.md`

Bao gồm:

- Project purpose.
- Phase hiện tại.
- Local quick start.
- VPS quick start.
- Các command Compose thường dùng.
- Link tới tài liệu chi tiết.

### `docs/architecture.md`

Bao gồm:

- Sơ đồ four-service topology.
- Trách nhiệm từng container.
- Dagster code-location boundary.
- Persistent storage boundary.

### `docs/local-development.md`

Bao gồm:

- Cài `uv`.
- Tạo local environment.
- Chạy tests.
- Chạy `dagster dev` nếu cần phát triển không qua full Compose.
- Chạy Compose locally để tái hiện VPS.

### `docs/vps-deployment.md`

Bao gồm:

- VPS prerequisites.
- Clone repository.
- Tạo `.env`.
- Build và start Compose.
- Kiểm tra services.
- Xem logs.
- Update deployment an toàn.
- Các cảnh báo firewall/public exposure.

---

## 15. Trình tự implementation

### Bước 1 — Khởi tạo repository và Python project

- Khởi tạo Git.
- Tạo `.gitignore`.
- Tạo `pyproject.toml`.
- Pin Python version.
- Thêm dependencies Dagster tối thiểu.
- Sinh `uv.lock`.

### Bước 2 — Tạo Dagster package tối thiểu

- Tạo `src/data_pipeline`.
- Tạo healthcheck asset.
- Tạo `definitions.py`.
- Xác minh import và Definitions validation.

### Bước 3 — Tạo placeholder directories

- `ingestion/stock/.gitkeep`.
- `ingestion/fund/.gitkeep`.
- `dbt/.gitkeep`.
- `migrations/.gitkeep`.
- `notebooks/.gitkeep`.
- Các storage placeholder.

### Bước 4 — Tạo test tối thiểu

- Test import package.
- Test load Definitions.
- Test healthcheck asset registration.

### Bước 5 — Tạo Docker image

- Cài locked dependencies.
- Cài project package.
- Chạy bằng production command có thể override từ Compose.
- Xác minh code server start được trong image.

### Bước 6 — Cấu hình Dagster instance

- Tạo `dagster.yaml`.
- Tạo `workspace.yaml`.
- Cấu hình PostgreSQL storage bằng environment variables.

### Bước 7 — Tạo Docker Compose stack

- PostgreSQL service và volume.
- Dagster code server.
- Dagster webserver.
- Dagster daemon.
- Healthchecks, dependencies và restart policies.

### Bước 8 — Viết scripts và documentation

- Deploy script.
- Logs script.
- Smoke-test script.
- Local development guide.
- VPS deployment guide.

### Bước 9 — Local verification

- Chạy tests.
- Build image.
- Start full Compose stack.
- Materialize healthcheck asset.
- Restart services và kiểm tra persistence.

### Bước 10 — VPS deployment verification

- Clone/pull repository lên VPS.
- Tạo production `.env`.
- Build và start stack.
- Kiểm tra firewall exposure.
- Materialize healthcheck asset.
- Recreate application services và xác nhận run history vẫn còn.

---

## 16. Completion criteria

Phase 1 hoàn tất khi tất cả điều kiện sau đạt được:

- [x] Repository sử dụng `src` layout và package import thành công.
- [x] Dependencies được pin bằng `uv.lock`.
- [x] Dagster `Definitions` load thành công.
- [ ] Healthcheck asset xuất hiện trong Dagster UI.
- [ ] Healthcheck asset materialize thành công.
- [ ] PostgreSQL container healthy.
- [ ] Dagster webserver healthy.
- [ ] Dagster daemon chạy ổn định.
- [ ] Dagster gRPC code location load thành công.
- [ ] Dagster run/event/schedule metadata dùng PostgreSQL storage.
- [ ] Run history tồn tại sau khi restart application containers.
- [ ] PostgreSQL data tồn tại sau khi recreate container với cùng volume.
- [x] `.env` thật và credentials không được commit.
- [x] dbt directory chỉ chứa `.gitkeep`.
- [x] migrations directory chỉ chứa `.gitkeep`.
- [x] notebooks directory chỉ chứa `.gitkeep`.
- [x] Chưa có stock/fund Dagster assets hoặc database loading trong Phase 1.
- [ ] README và VPS deployment guide phản ánh đúng command thực tế.

### Verification status sau lần init đầu tiên

- `uv sync`: thành công.
- `uv run pytest -q`: thành công, `3 passed`; bao gồm một local in-process materialization của healthcheck asset.
- Dagster Definitions validation: thành công cho `data_pipeline.definitions`.
- `docker compose config --quiet`: thành công với environment validation tạm thời.
- Docker image build và runtime verification: chưa thực hiện được vì Docker Desktop Linux engine không chạy trên máy local tại thời điểm kiểm tra.
- VPS deployment verification: chưa thực hiện.

### Verification status của standalone source probes

- `vnstock 4.0.8` và `httpx 0.28.1` được pin trong `uv.lock`.
- Unit tests cho stock và fund adapters: thành công.
- Live `FPT` crawl qua vnstock/KBS từ `2026-09-01` đến `2026-09-19`: thành công, 12 records.
- Live `E1VFVN30` crawl qua SSI trong cùng khoảng ngày: thành công, 12 records.
- Hai source cùng trả coverage từ `2026-09-03` đến `2026-09-18` trong lần kiểm tra.
- Raw live-test outputs được lưu dưới `storage/raw/` và bị Git ignore.
- Source runtime defaults đã được tập trung trong `.env`/`.env.example` và được
  dùng chung bởi standalone scripts; CLI arguments vẫn override theo từng run.
- `uv run pytest -q`: thành công, `8 passed` sau khi bổ sung validation cho
  source environment settings.
- `docker compose config --quiet`: thành công với `.env` local; các source
  settings đã được truyền vào Dagster containers để tái sử dụng khi tạo assets.
- Đã đăng ký `raw/stock_daily` và `raw/fund_daily` theo daily partitions, job
  `daily_market_ingestion`, và schedule 06:00 `Asia/Ho_Chi_Minh` chọn partition
  của ngày trước đó. Schedule mặc định ở trạng thái stopped để bật có chủ đích
  trên Dagster UI.
- Stock adapter dùng date range inclusive ở boundary của project và tự chuyển
  thành exclusive end date khi gọi vnstock/KBS; daily partition một ngày đã được
  live-test thành công với dữ liệu `FPT` ngày `2026-09-18`.
- Raw landing output dùng bind mount `./storage/raw` trên VPS thay cho Docker
  named volume, giúp inspect và backup trực tiếp từ host; runtime payloads vẫn
  bị Git ignore.

---

## 17. Rủi ro và biện pháp

### Dagster UI bị expose công khai

Rủi ro:

- Dagster OSS deployment không nên được xem như một public authenticated application mặc định.

Biện pháp Phase 1:

- Giới hạn bằng VPS firewall, private network hoặc SSH tunnel.
- Nginx/TLS/authentication được xử lý ở phase deployment-hardening riêng.

### PostgreSQL volume permissions

Rủi ro:

- Container không ghi được volume hoặc volume bị tạo với owner không phù hợp.

Biện pháp:

- Dùng official PostgreSQL image và named volume trong Phase 1.
- Không mount tùy tiện host directory vào PostgreSQL data directory.

### Code location load failure

Rủi ro:

- Sai module path, package chưa được install hoặc gRPC hostname sai.

Biện pháp:

- Test import trong image.
- Definitions validation trong pytest.
- Smoke test workspace/code location sau deploy.

### Dependency drift giữa local và VPS

Biện pháp:

- Cùng Python version.
- Cùng `uv.lock`.
- VPS build từ repository commit cụ thể.

### Over-scaffolding

Rủi ro:

- Tạo nhiều module rỗng khiến kiến trúc có vẻ đã được quyết định hoặc triển khai.

Biện pháp:

- Chỉ tạo Python files cần để Dagster chạy.
- Các capability tương lai dùng `.gitkeep`.

---

## 18. Phase tiếp theo

Sau khi Phase 1 ổn định trên VPS, thứ tự mở rộng dự kiến:

1. Thiết kế và tạo Bronze migrations.
2. Triển khai shared ingestion metadata.
3. Triển khai `vnstock` stock ingestion asset.
4. Triển khai SSI fund ingestion asset.
5. Thêm schedules, partitions và asset checks.
6. Thêm raw persistent storage.
7. Khởi tạo dbt project trong `src/data_pipeline/dbt/`.
8. Tích hợp dbt assets vào Dagster.

Mỗi phase chỉ mở rộng từ Dagster deployment đã được xác minh, không thay đổi topology nền tảng nếu không có nhu cầu vận hành thực tế.
