# el REST API（供 Orchestrator 控制）設計文件

- 日期：2026-07-08
- 範圍：為 `el` EL pipeline 加上 RESTful API，讓外部 Orchestrator 觸發與查詢拋轉
- 前置：延續 [2026-06-30-dlt-el-clickhouse-design.md](2026-06-30-dlt-el-clickhouse-design.md)
  與 [2026-07-08-batch-child-tables-design.md](2026-07-08-batch-child-tables-design.md)
- 狀態：規畫已認可，待寫實作計畫

## 1. 目標與範圍

提供 HTTP 介面，讓 Orchestrator 觸發指定來源的拋轉並取得結果，同時可探索有哪些來源/表可觸發。

- **執行模型：同步阻塞** — `POST` 觸發後一路等到拋轉完成才回傳各表筆數。逾時與重試由
  Orchestrator 自行處理。
- **併發：每來源互斥** — 同一來源同時只允許一個執行中；重複觸發回 `409`。不同來源可並行。
- **認證：OAuth2 Token Introspection** — 每個受保護端點驗證傳入的 Bearer token。
- 只包裝現有 `el` 的能力，不改變拋轉語意（mode、子表、命名守衛等一律沿用）。

非目標（YAGNI）：非同步作業佇列 / job 存放區、執行歷史持久化、取消執行、Web UI、
多 worker 水平擴充、Orchestrator 端排程邏輯。

## 2. 技術選型

- **FastAPI + uvicorn**：同步端點以 `def` 定義，FastAPI 會在 threadpool 執行，故長時間拋轉
  不阻塞事件迴圈，且不同來源可並行；附帶自動 OpenAPI/Swagger 文件方便串接。
- **httpx**：呼叫 IdentityServer 的 discovery 與 introspection 端點。
- **Pydantic**（已安裝）：請求/回應模型。
- **單一 uvicorn worker** 執行，確保記憶體中的「每來源鎖」是唯一權威。

新增相依：`fastapi`、`uvicorn[standard]`、`httpx`。

## 3. 系統結構

新增與 `el/` 平行的 `api/` 套件（依賴 `el`，使 CLI 不必安裝 web 相依）：

```
dlt/
├── el/                     # 既有核心（CLI 與拋轉邏輯）
│   └── pipeline.py         # run_source() 改為回傳結構化結果（見 §6）
└── api/                    # 新增：服務層
    ├── __init__.py
    ├── app.py              # FastAPI app 與路由
    ├── auth.py             # OAuth2 introspection 相依（FastAPI dependency）
    ├── runner.py           # 每來源鎖 + 呼叫 el.run_source
    ├── models.py           # Pydantic 請求/回應模型
    └── config.py           # 讀取 IdentityServer / API 設定（來自 .env）
```

啟動：`uvicorn api.app:app --host $API_HOST --port $API_PORT --workers 1`

模組職責：
- `config.py`：唯一讀取 API 相關環境變數處，輸出設定物件。
- `auth.py`：解析 `Authorization: Bearer`，向 IdentityServer introspection 驗證，失敗丟對應 HTTP 錯誤。
- `runner.py`：每來源互斥鎖；非阻塞搶鎖失敗即回報 busy；成功則呼叫 `el.pipeline.run_source` 並回傳結果。
- `models.py`：`RunRequest`、`RunResult`、`TableResult`、`SourceInfo` 等。
- `app.py`：組裝路由、相依注入、例外對應到 HTTP 狀態碼。

## 4. 接口清單

| Method | 路徑 | 需驗證 | 說明 |
|--------|------|:---:|------|
| `GET`  | `/health` | ✕ | 存活檢查 |
| `GET`  | `/sources` | ✓ | 列出所有來源與其表/mode |
| `GET`  | `/sources/{name}` | ✓ | 單一來源詳情 |
| `POST` | `/sources/{name}/run` | ✓ | 同步觸發拋轉，阻塞至完成 |

### 4.1 `GET /health`
無需驗證。回應 `200`：`{"status": "ok"}`。

### 4.2 `GET /sources`
回應 `200`：來源陣列，每筆 `SourceInfo`：
```json
{
  "name": "IMS", "type": "mssql", "schema": "dbo", "target_schema": "raw_test",
  "tables": [
    {"name": "IMS_P_Case", "mode": "batch", "batch_column": "Edition",
     "children": [{"name": "CpDetail", "child_key": "PID", "parent_key": "ID", "children": []}]},
    {"name": "IMS_P_Edition", "mode": "full_replace"}
  ]
}
```
來源清單由 `el.settings.load_catalog()` 提供。

### 4.3 `GET /sources/{name}`
回單一 `SourceInfo`；來源不存在回 `404`。

### 4.4 `POST /sources/{name}/run`
Request body（皆選填）：
```json
{ "batch_value": "2025W34", "tables": ["IMS_P_Case"] }
```
成功回應 `200`（`RunResult`）：
```json
{
  "source": "IMS", "status": "success",
  "started_at": "2026-07-08T03:12:00Z", "finished_at": "2026-07-08T03:12:12Z",
  "duration_seconds": 12.3,
  "tables": [
    {"path": "IMS_P_Case", "mode": "batch", "batch_value": "2025W34",
     "select": 791, "delete": 0, "insert": 791},
    {"path": "IMS_P_Case > CpDetail", "mode": "batch-child",
     "select": 2310, "delete": 0, "insert": 2310}
  ]
}
```

## 5. 認證（OAuth2 Token Introspection）

- 受保護端點需 `Authorization: Bearer <access_token>`。
- API 從 `{IDENTITY_SERVER_HOST}/.well-known/openid-configuration` 探索 `introspection_endpoint`
  （啟動時取得並快取；失敗時延遲到首次請求再試）。
- 以 env 的 `client_id` / `client_secret` 對 introspection 端點做 client 認證，送入待驗 token，
  檢查回應 `active == true`；若設定了 `OAUTH_REQUIRED_SCOPE`，另檢查 token 的 `scope` 是否包含之。
- 結果：無/格式錯誤的 Authorization → `401`；`active=false` 或 introspection 失敗 → `401`；
  scope 不足 → `403`。
- introspection 的網路呼叫用 httpx，設合理逾時；IdentityServer 不可達時回 `503`。

`.env` 新增鍵：
```dotenv
IDENTITY_SERVER_HOST=https://ids.giantplus.com   # discovery/introspection 的 authority
IDENTITY_SERVER_CLIENT_ID=el-api                  # API 作為 introspection client 的識別
IDENTITY_SERVER_CLIENT_SECRET=...
OAUTH_REQUIRED_SCOPE=el.run                       # 選填；未設則只檢查 active
API_HOST=0.0.0.0
API_PORT=8080
```

## 6. 核心改動：`run_source` 回傳結構化結果

`el.pipeline.run_source` 目前回傳 `None`、僅寫 log。改為**回傳結果物件**（並保留既有 log）：

- `RunResult`：`source`、`status`、`started_at`、`finished_at`、`duration_seconds`、`tables: list[TableResult]`。
- `TableResult`：`path`、`mode`、`batch_value`（batch 類才有）、`select`、`delete`、`insert`。

筆數來源沿用既有 `el.ch_internal.row_counts_for` 與 `delete_batch_tree` 的回傳。CLI（`el/run.py`）
改為呼叫後印出摘要，行為與現況等價。此為唯一動到 `el` 核心的改動，介面單純、可獨立測試。

## 7. 併發控制

- `api/runner.py` 維護「來源名稱 → 執行中旗標」的執行緒安全登錄表（單一全域鎖保護）。
- `POST /sources/{name}/run`：非阻塞嘗試將該來源標記為執行中；已在執行 → 回 `409`。
- 執行結束（成功或失敗）於 `finally` 清除旗標。
- 前提：單一 uvicorn worker。文件（README）需標明此限制。

## 8. 錯誤處理與狀態碼

| 狀況 | 狀態碼 |
|------|:---:|
| 成功 | `200` |
| 參數/設定驗證錯誤（sources.yml 無效、body 格式錯） | `400` |
| 無 / 無效 Bearer token、introspection `active=false` | `401` |
| scope 不足 | `403` |
| 來源不存在 | `404` |
| 該來源正在執行 | `409` |
| 拋轉失敗（DB 錯誤、命名漂移 `RuntimeError`、dlt 失敗） | `500` |
| IdentityServer 不可達 | `503` |

`500` 回應包含 `status: "failed"`、`error`（訊息）、以及可得知時的 `failed_table`。
失敗時 runner 仍會在 `finally` 釋放來源鎖。

## 9. 測試策略

- `api/auth.py`：以假的 introspection 回應驗證 active/inactive/scope 檢查與對應狀態碼。
- `api/runner.py`：驗證同來源重入被拒（409）、不同來源可並行、失敗後鎖被釋放。
- `api/app.py`：以 FastAPI `TestClient` + mock `run_source` 驗證各端點與狀態碼對應（不實連 DB）。
- `el.run_source`：驗證回傳的 `RunResult` 結構與筆數（可沿用既有的結構化/功能測試方式）。
- 端對端（人工）：對測試來源實際 `POST /sources/{name}/run`，比對回傳筆數與 log。

## 10. 待確認 / 開放細節

- introspection 逾時秒數與 discovery 快取策略（預設：啟動時取一次、失敗則首次請求重試；逾時 5s）。
- `POST` 是否接受以 query string 傳 `batch_value`/`tables`（預設只用 JSON body）。
- 是否需要在回應加入 dlt 的 `load_id`（預設不加，保持精簡；日後可從 trace 補上）。
