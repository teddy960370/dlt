# batch 模式：主從（父子）子表連動拋轉 設計文件

- 日期：2026-07-08
- 範圍：延伸現有 batch 模式，讓 batch 父表可連動拋轉遞迴巢狀的子表
- 前置：延續 [2026-06-30-dlt-el-clickhouse-design.md](2026-06-30-dlt-el-clickhouse-design.md)
- 狀態：設計已認可，待寫實作計畫

## 1. 目標與範圍

batch 模式抓取父表（如 Orders，依 `batch_column = value` 過濾）時，需同步抓取其子表
（如 SaleItems）中、透過外鍵關聯到本批次父列的資料；子表可再往下遞迴（父→子→孫…）。
父子關聯欄位隨資料庫/資料表而異，須可於設定指定。

- **延伸 batch 模式**，不新增 mode：子表的抽取邊界完全由父表的 batch 決定，複用既有的
  batch 值解析、預刪除、append 機制。
- 子表僅在父表底下巢狀定義，不另列為 top-level `tables`。
- 每個子表在 ClickHouse 仍是獨立實體表（如 `sale_items`），只是抽取時被父子關聯過濾。

非目標（YAGNI）：複合（多欄）關聯鍵、子表自身的 batch/edition 欄位、多 edition 並行。

## 2. 設定格式

batch 模式的表可加 `children`；每個 child 可再遞迴帶自己的 `children`。
關聯為單欄：`child_key`（子表 FK 欄）對應 `parent_key`（父表被參照欄）。

```yaml
  ORD:
    type: mssql
    schema: dbo
    target_schema: raw_ord
    tables:
      - name: Orders
        mode: batch
        batch_column: Edition
        children:
          - name: SaleItems
            child_key: PID          # SaleItems 上的 FK 欄
            parent_key: ID          # 對應 Orders 的欄
            children:               # 可再往下遞迴
              - name: SaleItemDetail
                child_key: ItemPID
                parent_key: ItemID  # 對應 SaleItems 的欄
```

驗證規則：
- `children` 只允許出現在 `mode: batch` 的表。
- 每個 child 必填 `name`、`child_key`、`parent_key`。
- child 不得有 `mode` / `batch_column`（其邊界由父表決定）。

## 3. 抽取過濾：遞迴「歸屬過濾」

定義 `membership(node)` 為「屬於本批次的列」的過濾條件：

- 根（batch 表）：`batch_column = value`
- 子表：`child_key IN (SELECT parent_key FROM parent WHERE membership(parent))`

每個節點的抽取 WHERE 即其 `membership(node)`。範例（SaleItemDetail）：

```sql
ItemPID IN (
  SELECT ItemID FROM SaleItems WHERE PID IN (
    SELECT ID FROM Orders WHERE Edition = :value))
```

實作：以 SQLAlchemy 子查詢遞迴組成。節點自身欄位用 dlt 傳入 `query_adapter_callback`
的反射 table；祖先表另外反射並快取（每個 `(schema, table)` 反射一次）。所有識別名沿用
既有 `_norm()`（`engine.dialect.normalize_name`）處理大小寫（Oracle 需要）。

## 4. ClickHouse 預刪除：同形狀、最深層先刪

以後序（子孫先於祖先）走訪整棵樹，對每個節點執行：

```sql
DELETE FROM <node_ch> WHERE <membership_ch(node)>
```

`membership_ch(node)` 與來源同形狀，但子查詢打在 ClickHouse 的祖先表上：

- 根：`<batch_col> = <literal>`
- 子表：`<child_key> IN (SELECT <parent_key> FROM <parent_ch> WHERE <membership_ch(parent)>)`

因為後序刪除，刪某節點時其祖先在 CH 的舊批次資料尚未被刪，能正確定位要刪的列。
節點的 CH 實體表若尚不存在（首次載入）則略過該節點的刪除。實體表名與欄名以 dlt naming
（`make_qualified_table_name` / `normalize_*`）計算；字面值用既有 `_ch_literal()`。

刪除筆數：對每個節點於刪除前先 `SELECT count()`，作為該節點的 `delete` 筆數記錄。

## 5. 載入順序與 Log

- 載入：前序（父先、子後），每個節點各自 `pipeline.run(resource)`，`write_disposition="append"`。
- Log：沿用既有 `select / delete / insert` 筆數，**每個節點各一行**，標示節點路徑，例如
  `[ORD.Orders > SaleItems] mode=batch-child | select=.. delete=.. insert=..`。

## 6. 模組異動

- `settings.py`：
  - 新增 `ChildConfig`（`name`、`child_key`、`parent_key`、`children: list[ChildConfig]`）。
  - `TableConfig` 加 `children: list[ChildConfig]`。
  - `load_catalog` 遞迴解析並驗證（見第 2 節規則）。
- `source.py`：
  - 新增祖先表反射快取。
  - 新增 `membership()` 遞迴建構 SQLAlchemy 過濾；子表 resource 以此為 `query_adapter_callback`。
  - 提供將 batch 表展開為節點清單（含路徑、父節點、關聯鍵）的輔助函式。
- `batch.py`：新增 `membership_ch()` 與後序刪除（含存在檢查、刪前 count）。
- `pipeline.py`：batch 表若有 children，展開所有節點；先後序預刪除、再前序載入，逐節點記數。

## 7. 錯誤處理

- 設定驗證失敗（缺 `child_key`/`parent_key`、children 用於非 batch 表）：載入設定時明確報錯。
- 祖先表反射失敗（表/owner 不存在）：明確標示節點路徑後中止。
- CH 節點表不存在：略過該節點刪除（首次載入情境）。
- 任一節點載入失敗：沿用 dlt load package 機制，不靜默吞錯。

## 8. 邊界情境

- 重跑同一批次：後序刪除先清掉本批次父列所關聯的子孫列，再刪父列，接著重新載入 →
  不會殘留孤兒列。
- 父列在新批次中消失：刪除以 CH 現存「舊批次父列」為基準辨識子孫，仍能清乾淨。
- 子表出現不屬於任何本批次父列的列：依定義不會被抽取（membership 過濾）。

## 9. 測試策略

- `settings.py`：驗證遞迴 children 解析與驗證規則（缺鍵、children 用於非 batch）。
- `source.py`：驗證 `membership()` 對單層/多層產生正確的巢狀 WHERE（比對編譯後 SQL）。
- `batch.py`：驗證 `membership_ch()` 巢狀 SQL 組裝與後序刪除順序。
- `pipeline.py`：以 stub 驗證節點展開、預刪除（後序）與載入（前序）順序、逐節點記數。
- 端對端：父+兩層子表，驗證重跑不殘留、子孫僅含本批次關聯列。
