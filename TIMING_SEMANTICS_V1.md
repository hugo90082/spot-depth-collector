# 正式取樣時間語意

- 固定 UTC 秒格排程，只在名義時間格 T 到達後取樣，不提前取樣。
- scheduler callback 若已晚於 T 超過 50ms，該名義 slot 直接視為 missed，不補值、不 carry-forward。
- 每個 venue × asset 在真正讀取其 book 前獨立取得 `actualTs = Date.now()`；12 個來源不再共用同一個時間戳。
- block 內仍只保存已鎖定的 uint32 block-relative `actualTs`，不新增 nominal timestamp 欄位，以免改變 `SPOT_DEPTH_STORAGE_V1` 實體格式。
- nominal slot 可由 record kind 的固定 cadence（Near 1s / Mid 2s / Far 5s / Persistence 5s）與 actual timestamp 決定性重建。
- `abs(actualTs - nominalTs) > 50ms` 的資料在衍生跨交易所聚合時標記為 `ALIGNMENT_INVALID`。
- runtime telemetry 額外監測 30 秒窗內最大 grid offset、各資產四家來源最大 skew、以及 missed scheduler slots；telemetry 不進研究 payload。
