import http from 'node:http';
import {spawn} from 'node:child_process';
import {DATASET_DIR,DOWNLOAD_TOKEN,PORT} from '../config.mjs';

export function startHttp(statusFn){
  const server=http.createServer((req,res)=>{
    const u=new URL(req.url,`http://${req.headers.host||'localhost'}`);
    if(u.pathname==='/health'){
      res.setHeader('content-type','application/json');
      return res.end(JSON.stringify({ok:true,...statusFn()}));
    }
    if(u.pathname==='/status'){
      res.setHeader('content-type','application/json; charset=utf-8');
      return res.end(JSON.stringify(statusFn(),null,2));
    }
    if(u.pathname==='/download'){
      if(DOWNLOAD_TOKEN&&u.searchParams.get('token')!==DOWNLOAD_TOKEN){res.statusCode=403;return res.end('forbidden');}
      const s=statusFn();
      if(!s.collectionId){res.statusCode=404;return res.end('no dataset');}
      res.setHeader('content-type','application/gzip');
      res.setHeader('content-disposition',`attachment; filename="spot-depth-${s.collectionId}.tar.gz"`);
      const p=spawn('tar',['-czf','-','-C',DATASET_DIR,'.']);
      p.stdout.pipe(res);
      p.stderr.on('data',()=>{});
      p.on('close',c=>{if(c!==0&&!res.headersSent)res.statusCode=500;});
      return;
    }
    const s=statusFn();
    const usedPct=s.capacityBytes?((s.volumeBytes||0)/s.capacityBytes*100).toFixed(3):'0';
    const days=((s.collectionElapsedMs||0)/86400000).toFixed(3);
    const download=DOWNLOAD_TOKEN?'下載已設保護碼；完成後使用專用下載連結。':'<a href="/download">下載目前資料（tar.gz）</a>';
    res.setHeader('content-type','text/html; charset=utf-8');
    res.end(`<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta http-equiv="refresh" content="30"><title>現貨深度採集</title><style>body{font-family:system-ui;max-width:960px;margin:40px auto;padding:0 20px}pre{background:#111;color:#eee;padding:16px;overflow:auto}a{font-size:18px}table{border-collapse:collapse}td{padding:4px 14px 4px 0}</style></head><body><h1>聚合現貨深度資料採集</h1><table><tr><td>狀態</td><td><b>${escapeHtml(String(s.status))}</b></td></tr><tr><td>Collection ID</td><td>${escapeHtml(String(s.collectionId||'-'))}</td></tr><tr><td>目前分段</td><td>${escapeHtml(String(s.runId||'-'))}</td></tr><tr><td>已採集</td><td>${days} 天</td></tr><tr><td>Volume 使用</td><td>${usedPct}% (${s.volumeBytes||0} / ${s.capacityBytes||0} bytes)</td></tr></table><p>${download}</p><pre>${escapeHtml(JSON.stringify(s,null,2))}</pre></body></html>`);
  });
  server.listen(PORT,'0.0.0.0');
  return server;
}
function escapeHtml(s){return s.replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));}
