export const MIXED14 = [0,5,10,25,50,100,200,300,500,750,1000,1250,1500,1750,2000];
export const ASSETS = ['BTC','ETH','SOL'];
export const VENUES = ['binance','coinbase','kraken','bitfinex'];
export const PAIRS = {
  binance: {BTC:'BTCUSDT',ETH:'ETHUSDT',SOL:'SOLUSDT'},
  coinbase:{BTC:'BTC-USD',ETH:'ETH-USD',SOL:'SOL-USD'},
  kraken:  {BTC:'BTC/USD',ETH:'ETH/USD',SOL:'SOL/USD'},
  bitfinex:{BTC:'tBTCUSD',ETH:'tETHUSD',SOL:'tSOLUST'}
};
export const KRAKEN_REST = {
  BTC:{pair:'XBTUSD',grouping:250}, ETH:{pair:'ETHUSD',grouping:25}, SOL:{pair:'SOLUSD',grouping:1}
};
export const BITFINEX_BROAD_PREC = {BTC:'P2',ETH:'P2',SOL:'P0'};
export const BITFINEX_NEAR_PREC = {BTC:'P0',ETH:'P0',SOL:'P0'};
export const BLOCK_MS = 300_000;
export const MAX_RUN_MS = 10*24*60*60*1000;
export const DATA_DIR = process.env.DATA_DIR || '/data';
export const DATASET_DIR = process.env.DATASET_DIR || `${DATA_DIR}/formal-dataset-v1`;
export const CAPACITY_BYTES = Number(process.env.CAPACITY_BYTES || 500_000_000);
export const STOP_RESERVE_BYTES = Number(process.env.STOP_RESERVE_BYTES || 8_000_000);
export const PORT = Number(process.env.PORT || 3000);
export const DOWNLOAD_TOKEN = process.env.DOWNLOAD_TOKEN || '';
