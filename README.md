# Spot Depth Collector v1

正式採集器骨架，對應目前專案已鎖定規格：

- Binance / Coinbase / Kraken / Bitfinex
- BTC / ETH / SOL
- Mixed14 shells, ±2000 bps
- Near 1s / Mid 2s / Far 5s
- Persistence 10s / 30s every 5s
- venue × asset × 5-minute gzip block
- 10 天或安全容量上限先到者停止
- 不滾動刪除
- dataset-manifest.json
- `/download` 串流整包 tar.gz，不額外占 Volume 容量

## Railway

必要設定：

- Volume mount: `/data`
- `CAPACITY_BYTES=500000000`
- `STOP_RESERVE_BYTES=8000000`
- 可選 `DOWNLOAD_TOKEN=<random>`

啟動：`npm start`

健康檢查：`/health`
狀態頁：`/`
下載：`/download?token=...`

## 目前驗證狀態

此程式包已通過本地 5 項核心語意測試，但**尚未完成 Railway 真實 feed 30–60 分鐘 replay 驗證**。部署後第一階段仍應視為 production candidate，而不是直接宣告 10 天正式資料有效。
