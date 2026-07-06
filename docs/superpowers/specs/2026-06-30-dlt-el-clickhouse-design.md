# dlt EL → ClickHouse 設計文件

- 日期：2026-06-30
- 範圍：使用 dlt 將 MSSQL / Oracle 來源資料拋轉（Extract-Load）至 ClickHouse
- 狀態：設計確認，待寫實作計畫

## 1. 目標與範圍

將不同來源資料庫（MSSQL、Oracle，且**可能有多個實例**）的指定資料表，以 dlt 拋轉至 ClickHouse。

- 本階段**只做 EL（Extract-Load）**，資料轉換（Transform）未來交給 dbt，不納入此次結構。
- 來源以**具名實例**定義（如 `MSSQL1`、`MSSQL2`、`Oracle1`、`Oracle2`…），數量可擴充，實際名稱與連線由開發人員未來補上。
- 每個來源實例以**固定資料表清單**定義，不做整庫反射。
- 每張表依其「拋轉模式」決定寫入策略（見第 3 節）。
- 所有連線資訊以 **dotenv（.env）** 集中管理。

非目標（YAGNI）：dbt 轉換、整庫自動反射、增量（incremental）載入、多 batch 並行載入。

## 2. 系統結構

> 注意：repo 根資料夾名為 `dlt`，與 dlt 函式庫同名。程式套件**不可**命名為 `dlt`（會遮蔽函式庫），故採用 `el/`。

```
dlt/                          # repo 根目錄
├── .env.example              # 連線資訊範本（提交 git）
├── .env                      # 實際連線資訊（git 忽略）
├── .gitignore
├── requirements.txt
├── config/
│   └── sources.yml           # 來源實例清單：每個實例的 type、schema、固定資料表清單與每張表的 mode
├── el/                       # 程式套件（Extract-Load）
│   ├── __init__.py
│   ├── settings.py           # python-dotenv 載入 .env，依來源實例名稱解析連線設定
│   ├── connections.py        # 依 type 組 MSSQL/Oracle 連線、建立 ClickHouse client / dlt destination
│   ├── batch.py              # 解析 batch value（CLI 優先，否則抓最新）+ ClickHouse 刪除/存在檢查
│   ├── source.py             # 依設定建立 dlt sql_table resource（含 batch 過濾與寫入策略）
│   ├── pipeline.py           # 核心流程：刪除舊資料 → dlt 寫入
│   └── run.py                # CLI 進入點（argparse）
└── docs/superpowers/specs/   # 設計與計畫文件
```

模組職責邊界：

- `settings.py`：唯一讀取 `os.environ` / `.env` 的地方。依「來源實例名稱」當前綴解析來源連線設定，並讀取單一 ClickHouse 設定，輸出 typed 物件（`SourceConfig`、`ClickHouseConfig`）。
- `connections.py`：把設定物件依 `type` 轉成連線 — 來源用 SQLAlchemy engine／dlt 憑證；ClickHouse 用 clickhouse-connect client 與 dlt clickhouse destination。
- `batch.py`：batch value 的解析與 ClickHouse 的刪除/存在檢查，純函式，輸入連線、表名與 batch 欄位。
- `source.py`：把一張表的設定（schema、table、mode、batch 欄位/值）轉成一個 dlt resource。
- `pipeline.py`：依來源實例逐表編排「刪除 → 寫入」，不直接碰 .env。
- `run.py`：只負責解析 CLI 參數並呼叫 `pipeline`。

## 3. 拋轉模式

每張表在 `config/sources.yml` 以 `mode` 標註，共三種：

| 模式 | 用途 | 需要參數 | 寫入策略 |
|------|------|---------|---------|
| `batch` | 一批一批進來的版本／批次資料 | `batch_column`（欄位名，表設定）＋ batch value（CLI 帶入，空則抓最新） | 來源以 `WHERE <batch_column> = <value>` 過濾；ClickHouse 先 `DELETE WHERE <batch_column> = <value>` 再 append |
| `full_replace` | 維度表（資料量小） | 無 | 來源全表讀取；ClickHouse 整表清空後全量寫入（dlt `write_disposition="replace"`） |
| `scd2` | 緩時變維度（Slowly Changing Dimension Type 2），保留歷史版本 | `scd_natural_key`（自然／業務鍵，可多欄） | 來源讀取目前完整快照；dlt `write_disposition={"disposition":"merge","strategy":"scd2"}`，自動維護有效期欄位 |

### 3.1 batch 模式流程
1. 取得該表的 `batch_column`（來自 `sources.yml`）。
2. 解析 batch value（見第 4 節）：CLI 帶入則用該值；未帶則對該表查 `SELECT MAX(<batch_column>)` 取最新。
3. 若目標表已存在於 ClickHouse，執行 `DELETE FROM <db>.<table> WHERE <batch_column> = <value>`；表不存在則略過（首次載入由 dlt 自動建表）。
4. dlt 以 `sql_table` resource 從來源讀取，透過 `query_adapter_callback` 加上 `WHERE <batch_column> = <value>`，以 `write_disposition="append"` 寫入。

### 3.2 full_replace 模式流程
1. dlt 以 `sql_table` resource 讀取整張來源表（不加任何過濾）。
2. 以 `write_disposition="replace"` 寫入；dlt 會在載入時清空目的表後全量寫入。

### 3.3 scd2 模式流程（緩時變維度）
1. 取得該表的 `scd_natural_key`（來自 `sources.yml`），設為 dlt resource 的自然鍵（`merge_key`），用來判斷同一維度成員的版本變化。
2. dlt 以 `sql_table` resource 讀取目前完整快照（不加 batch 過濾、不預刪除）。
3. 以 `write_disposition={"disposition": "merge", "strategy": "scd2"}` 寫入；dlt 自動：
   - 對既有但內容已變的列關閉舊版本（填 `_dlt_valid_to`），插入新版本（`_dlt_valid_from` = 載入時間）。
   - 對來源新出現的列建立首版。
   - 有效期欄位預設 `_dlt_valid_from` / `_dlt_valid_to`（可由 `validity_column_names` 自訂）。
4. 已驗證：安裝中的 dlt 1.28.1 ClickHouse destination `supported_merge_strategies = ["delete-insert", "scd2"]`，原生支援此模式，無需自行實作。

### 3.4 為何 batch 模式選「明確預刪除 + append」而非 dlt merge/replace
（此節僅針對 batch 模式；scd2 模式刻意使用 dlt merge/scd2，見 3.3。）
- `replace`：會清掉整張表（含其他批次），只適用 full_replace 模式，不適用 batch 模式。
- `merge`：依主鍵去重，無法刪掉「新批次中沒有、但屬於該 batch value 的舊列」，語意不符 batch 的「整批覆蓋」需求。
- **明確預刪除 + append**：精準刪除該 batch value 的所有列，完全可控，且不受 dlt ClickHouse 目的地對 merge 支援度的限制。

## 4. batch value 解析

batch 模式的每張表在 `sources.yml` 指定 `batch_column`（要當批次鍵的欄位名）。批次值（value）的來源：

- CLI 帶入 `--batch-value <值>`：直接使用該值。
- 未帶入（空值）：對該來源實例中**每一張 batch 模式的表**各自查詢 `SELECT MAX(<batch_column>) FROM <schema>.<table>`，取其最新值載入。
  - 設計理由：不同表的 `batch_column` 與最新值不保證相同，逐表解析最安全。若需所有表使用同一批次值，請以 `--batch-value` 明確帶入。
- `full_replace` 模式的表不使用 batch value。

## 5. 連線管理（dotenv）

- 使用 `python-dotenv` 載入 `.env` 到環境變數，於 `settings.py` 集中讀取。
- 採用乾淨的自訂鍵名，**不使用** dlt 的 `secrets.toml`；在 `connections.py` 明確組出連線字串與憑證。

### 5.1 多來源實例命名慣例
來源連線以「實例名稱（大寫）」當前綴，因此可任意新增實例而不改程式。實例名稱與其在 `sources.yml` 的 key 一致。

每個來源實例的 `.env` 鍵（以 `<NAME>` 代表實例名稱，如 `MSSQL1`、`ORACLE1`）：

MSSQL 型實例：
```dotenv
<NAME>_HOST=
<NAME>_PORT=1433
<NAME>_DATABASE=
<NAME>_USER=
<NAME>_PASSWORD=
<NAME>_ODBC_DRIVER=ODBC Driver 18 for SQL Server
```

Oracle 型實例：
```dotenv
<NAME>_HOST=
<NAME>_PORT=1521
<NAME>_SERVICE_NAME=
<NAME>_USER=
<NAME>_PASSWORD=
```

ClickHouse（單一目的地）：
```dotenv
CLICKHOUSE_HOST=
CLICKHOUSE_HTTP_PORT=8123
CLICKHOUSE_PORT=9000
CLICKHOUSE_DATABASE=
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_SECURE=false
```

`.env.example` 會放每種 type 各一組範例（如 `MSSQL1_*`、`ORACLE1_*`）供開發人員複製改名。

### 5.2 連線字串型式
- MSSQL（SQLAlchemy + pyodbc）：
  `mssql+pyodbc://USER:PASS@HOST:PORT/DB?driver=<ODBC Driver 18 for SQL Server>&TrustServerCertificate=yes`
- Oracle（SQLAlchemy + oracledb，thin 模式）：
  `oracle+oracledb://USER:PASS@HOST:PORT/?service_name=<SERVICE>`
- ClickHouse：以環境變數組出 dlt clickhouse destination 憑證；刪除/存在檢查使用 clickhouse-connect client（HTTP）。

### 5.3 sources.yml 格式
```yaml
sources:
  MSSQL1:                      # 來源實例名稱（= .env 前綴、= CLI --source 值）
    type: mssql                # mssql | oracle，決定驅動與連線字串
    schema: dbo
    tables:
      - name: sales_fact
        mode: batch
        batch_column: edition
      - name: dim_product
        mode: full_replace
      - name: dim_customer
        mode: scd2
        scd_natural_key: customer_id   # 自然鍵，可寫成清單支援多欄
  ORACLE1:
    type: oracle
    schema: APP
    tables:
      - name: ORDER_FACT
        mode: batch
        batch_column: BATCH_NO
```

## 6. Metadata

採用 **dlt 內建血緣 metadata**，不另加自訂欄位：

- 每張載入的表自動帶 `_dlt_load_id`、`_dlt_id`。
- 系統表 `_dlt_loads`（含 `load_id`、`schema_name`、`status`、`inserted_at`）記錄每批次載入資訊；以 `_dlt_load_id` join 可取得載入時間與狀態。
- scd2 模式的表另由 dlt 自動維護有效期欄位 `_dlt_valid_from` / `_dlt_valid_to`。

## 7. 套件相依（需補進 requirements.txt）

目前 requirements.txt 已含 `dlt`、`clickhouse-connect`、`clickhouse-driver`，但缺：

- `sqlalchemy` — dlt `sql_database` source 必要。
- `pyodbc` — MSSQL 驅動。**OS 層需另行安裝 Microsoft ODBC Driver 18 for SQL Server**（非 pip 套件）。
- `oracledb` — Oracle 驅動（thin 模式免裝 Oracle Client）。
- `python-dotenv` — 讀取 .env。

## 8. CLI 介面

```
python -m el.run --source <實例名稱> [--batch-value <值>] [--tables <t1,t2,...>]
```

- `--source`（必填）：要拋轉的來源實例名稱，對應 `sources.yml` 的 key（如 `MSSQL1`、`ORACLE1`）。
- `--batch-value`（選填）：batch 模式表使用；未帶則各表抓各自 `batch_column` 的最新值。
- `--tables`（選填）：只跑指定子集；未帶則跑該來源實例清單中所有表，各自依 mode 處理。

## 9. 錯誤處理

- 來源連線/查詢失敗：明確拋出並標示來源實例名稱與表名，該次執行中止。
- ClickHouse 刪除前先確認表存在；不存在則略過刪除（首次載入情境）。
- dlt 載入失敗：沿用 dlt 的 load package 機制，記錄失敗的 load_id，不靜默吞錯。

## 10. 測試策略

- `settings.py`：以注入的環境變數驗證來源實例設定解析（依名稱前綴、含預設值）與 ClickHouse 設定。
- `batch.py`：以假的連線物件驗證 batch value 解析（CLI 優先、否則 MAX(batch_column)）與刪除 SQL 組裝。
- `source.py`：驗證 batch 模式會掛上 `WHERE <batch_column> = <value>` 過濾與正確的 write_disposition。
- `source.py`（scd2）：驗證 scd2 模式套用 `merge`/`scd2` write_disposition 與正確的自然鍵。
- `pipeline.py`：以 stub 驗證模式分流（batch → 預刪除+append；full_replace → replace；scd2 → merge/scd2，不預刪除）。
- 端對端：對測試用 ClickHouse 驗證 batch 重跑不重複、full_replace 全量覆蓋、scd2 改值後產生新版本且舊版本被關閉（`_dlt_valid_to` 填值）。

## 11. 待確認 / 開放細節

- ClickHouse 連線走 HTTP（8123/8443）為主；native port 保留於設定。
- 未帶 `--batch-value` 時逐表抓各自 `batch_column` 最新值的行為是否符合預期。
- 單一來源實例若同時含多張 batch 表且各自 `batch_column` 不同，`--batch-value` 會以同一值套用到各表（各自的欄位）。
- 是否需要「一次執行多個來源實例」（目前 `--source` 一次一個；多實例可由排程多次呼叫）。
