import crypto from 'node:crypto';
import fsp from 'node:fs/promises';
import {DATASET_DIR,CAPACITY_BYTES,STOP_RESERVE_BYTES,MAX_RUN_MS} from './config.mjs';
import {BlockWriter,directoryBytes} from './storage/block-writer.mjs';
import {Manifest} from './storage/manifest.mjs';
import {makeDepthSample,makePersistenceSample} from './core/sample.mjs';
import {startBinance} from './exchanges/binance.mjs';
import {startCoinbase} from './exchanges/coinbase.mjs';
import {startKraken} from './exchanges/kraken.mjs';
import {startBitfinex} from './exchanges/bitfinex.mjs';
import {startHttp} from './server/http.mjs';

const runId=`${new Date().toISOString().replace(/[:.]/g,'-')}-${crypto.randomBytes(3).toString('hex')}`;
const startMs=Date.now();
await fsp.mkdir(DATASET_DIR,{recursive:true});
const writer=new BlockWriter(runId);
await writer.init();
const manifest=new Manifest(runId,startMs);
await manifest.init();

let status='STARTING',stopReason=null,stopping=false,totalBytes=await directoryBytes(DATASET_DIR);
const sourceMap=new Map();
const feeds=[];

function onEvent(s,type){
  manifest.event(type);
  writer.add(s.venue,s.asset,{type,actualTs:Date.now()});
  console.log(JSON.stringify({kind:'feed-event',type,venue:s.venue,asset:s.asset,ts:new Date().toISOString(),gaps:s.gaps,resets:s.resets}));
}

for(const factory of [startBinance,startCoinbase,startKraken,startBitfinex]){
  const f=factory(onEvent);
  feeds.push(f);
  for(const s of Object.values(f.states))sourceMap.set(`${s.venue}:${s.asset}`,s);
}
status='RUNNING';
console.log(JSON.stringify({kind:'collector-start',runId,startIso:new Date(startMs).toISOString(),datasetDir:DATASET_DIR,capacityBytes:CAPACITY_BYTES,reserveBytes:STOP_RESERVE_BYTES}));

function sourceSummary(){
  const out={};
  for(const [k,s] of sourceMap)out[k]={
    quality:s.quality,
    ready:!!s.broadBook?.ready,
    nearReady:!!s.nearBook?.ready,
    lastMsgAt:s.lastMsgAt,
    gaps:s.gaps,
    resets:s.resets
  };
  return out;
}

const state=()=>({
  status,runId,startMs,startIso:new Date(startMs).toISOString(),elapsedMs:Date.now()-startMs,
  totalBytes,capacityBytes:CAPACITY_BYTES,reserveBytes:STOP_RESERVE_BYTES,
  estimatedNextBatchBytes:writer.estimatedNextBatchBytes(),stopReason,sources:sourceSummary()
});
startHttp(state);

const telemetry=setInterval(()=>{
  const sources=sourceSummary();
  const readyCount=Object.values(sources).filter(s=>s.ready).length;
  const nearReadyCount=Object.values(sources).filter(s=>s.nearReady).length;
  console.log(JSON.stringify({kind:'collector-status',ts:new Date().toISOString(),status,runId,readyCount,nearReadyCount,totalSources:Object.keys(sources).length,totalBytes,estimatedNextBatchBytes:writer.estimatedNextBatchBytes(),sources}));
},30000);
telemetry.unref?.();

function sampleAt(target){
  const actual=Date.now();
  for(const s of sourceMap.values()){
    if(!s.broadBook?.ready)continue;
    const sec=Math.floor(target/1000)%60;
    if(sec%1===0){const r=makeDepthSample(s,actual,'near');if(r)writer.add(s.venue,s.asset,r);}
    if(sec%2===0){const r=makeDepthSample(s,actual,'mid');if(r)writer.add(s.venue,s.asset,r);}
    if(sec%5===0){
      const r=makeDepthSample(s,actual,'far');if(r)writer.add(s.venue,s.asset,r);
      const p=makePersistenceSample(s,actual);if(p)writer.add(s.venue,s.asset,p);
    }
  }
}

let next=Math.ceil(Date.now()/1000)*1000;
const tick=async()=>{
  if(stopping)return;
  const now=Date.now();
  while(next<=now+20){
    if(Math.abs(now-next)<=50)sampleAt(next);
    next+=1000;
  }
  const files=await writer.flushClosed(now);
  if(files.length){
    manifest.addFiles(files);
    await manifest.save();
    totalBytes=await directoryBytes(DATASET_DIR);
    console.log(JSON.stringify({kind:'block-flush',ts:new Date().toISOString(),fileCount:files.length,totalBytes,files:files.map(f=>({path:f.path,bytes:f.bytes,sha256:f.sha256}))}));
    const need=writer.estimatedNextBatchBytes()+STOP_RESERVE_BYTES;
    if(totalBytes+need>=CAPACITY_BYTES)return stop('STORAGE_LIMIT');
  }
  if(now-startMs>=MAX_RUN_MS)return stop('TEN_DAY_LIMIT');
  setTimeout(tick,Math.max(10,next-Date.now()-5));
};
tick();

async function stop(reason){
  if(stopping)return;
  stopping=true;
  status='STOPPING';
  stopReason=reason;
  console.log(JSON.stringify({kind:'collector-stop-begin',reason,ts:new Date().toISOString()}));
  for(const f of feeds)f.stop();
  const files=await writer.flushAll();
  manifest.addFiles(files);
  totalBytes=await directoryBytes(DATASET_DIR);
  await manifest.finish(reason,Date.now(),totalBytes);
  status='STOPPED';
  console.log(JSON.stringify({kind:'collector-stopped',reason,ts:new Date().toISOString(),totalBytes,fileCount:files.length}));
}

process.on('SIGTERM',()=>stop('SIGTERM'));
process.on('SIGINT',()=>stop('SIGINT'));
