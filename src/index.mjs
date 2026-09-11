import crypto from 'node:crypto';
import fsp from 'node:fs/promises';
import {DATA_DIR,DATASET_DIR,CAPACITY_BYTES,STOP_RESERVE_BYTES,MAX_RUN_MS} from './config.mjs';
import {BlockWriter,directoryBytes} from './storage/block-writer.mjs';
import {Manifest} from './storage/manifest.mjs';
import {CollectionState} from './storage/collection-state.mjs';
import {makeDepthSample,makePersistenceSample} from './core/sample.mjs';
import {startBinance} from './exchanges/binance.mjs';
import {startCoinbase} from './exchanges/coinbase.mjs';
import {startKraken} from './exchanges/kraken.mjs';
import {startBitfinex} from './exchanges/bitfinex.mjs';
import {startHttp} from './server/http.mjs';

await main();

async function main(){
  await fsp.mkdir(DATASET_DIR,{recursive:true});
  const collection=await CollectionState.openOrCreate();
  let volumeBytes=await directoryBytes(DATA_DIR);
  let datasetBytes=await directoryBytes(DATASET_DIR);

  if(collection.data.status==='STOPPED'){
    const state=()=>({status:'STOPPED',collectionId:collection.data.collectionId,runId:null,collectionStartMs:collection.data.startMs,collectionStartIso:collection.data.startIso,collectionElapsedMs:(collection.data.endMs||Date.now())-collection.data.startMs,volumeBytes,datasetBytes,capacityBytes:CAPACITY_BYTES,reserveBytes:STOP_RESERVE_BYTES,stopReason:collection.data.stopReason,segments:collection.data.segments.length,sources:{}});
    startHttp(state);
    console.log(JSON.stringify({kind:'collector-already-stopped',collectionId:collection.data.collectionId,stopReason:collection.data.stopReason}));
    return;
  }

  const runId=`${new Date().toISOString().replace(/[:.]/g,'-')}-${crypto.randomBytes(3).toString('hex')}`;
  const segmentStartMs=Date.now();
  await collection.beginSegment(runId,segmentStartMs);
  const writer=new BlockWriter(runId);await writer.init();
  const manifest=new Manifest(runId,segmentStartMs);await manifest.init();
  let status='STARTING',stopReason=null,stopping=false;
  const sourceMap=new Map(),feeds=[];
  let timingWindow=newTimingWindow();

  console.log(JSON.stringify({kind:'collector-start',collectionId:collection.data.collectionId,runId,segmentStartIso:new Date(segmentStartMs).toISOString(),collectionStartIso:collection.data.startIso,datasetDir:DATASET_DIR,capacityBytes:CAPACITY_BYTES,reserveBytes:STOP_RESERVE_BYTES}));

  function onEvent(s,type){
    manifest.event(type);writer.add(s.venue,s.asset,{type,actualTs:Date.now()});
    console.log(JSON.stringify({kind:'feed-event',type,venue:s.venue,asset:s.asset,ts:new Date().toISOString(),gaps:s.gaps,resets:s.resets}));
  }

  for(const factory of [startBinance,startCoinbase,startKraken,startBitfinex]){
    const f=factory(onEvent);feeds.push(f);
    for(const s of Object.values(f.states))sourceMap.set(`${s.venue}:${s.asset}`,s);
  }
  status='RUNNING';

  function sourceSummary(){
    const out={};
    for(const [k,s] of sourceMap){
      const nearQuality=s.qualityFor?.('near') ?? s.quality;
      const broadQuality=s.qualityFor?.('mid') ?? s.quality;
      out[k]={nearQuality,broadQuality,ready:!!s.broadBook?.ready,nearReady:!!s.nearBook?.ready,lastMsgAt:s.lastMsgAt,gaps:s.gaps,resets:s.resets};
    }
    return out;
  }
  const state=()=>({status,collectionId:collection.data.collectionId,runId,segmentStartMs,segmentStartIso:new Date(segmentStartMs).toISOString(),collectionStartMs:collection.data.startMs,collectionStartIso:collection.data.startIso,collectionElapsedMs:Date.now()-collection.data.startMs,volumeBytes,datasetBytes,capacityBytes:CAPACITY_BYTES,reserveBytes:STOP_RESERVE_BYTES,estimatedNextBatchBytes:writer.estimatedNextBatchBytes(),stopReason,segments:collection.data.segments.length,sources:sourceSummary()});
  startHttp(state);

  function newTimingWindow(){return {sourceSamples:0,maxAbsGridOffsetMs:0,over50SourceSamples:0,maxAssetSkewMs:0,maxSkewByAsset:{BTC:0,ETH:0,SOL:0},missedSchedulerSlots:0};}
  function sampleAt(target){
    const actualByAsset={BTC:[],ETH:[],SOL:[]};
    const captured=[];
    const sec=Math.floor(target/1000)%60;

    // Node 的 feed 更新與此同步 callback 不會交錯執行；先快速為所有 ready
    // 來源各自捕捉 actualTs，再做較昂貴的 shell 計算，才能讓時間戳代表
    // 同一個邏輯 book-state sampling instant，而不是計算順序耗時。
    for(const s of sourceMap.values()){
      if(!s.broadBook?.ready)continue;
      const actual=Date.now();
      captured.push({s,actual});
      const offset=Math.abs(actual-target);
      timingWindow.sourceSamples++;
      timingWindow.maxAbsGridOffsetMs=Math.max(timingWindow.maxAbsGridOffsetMs,offset);
      if(offset>50)timingWindow.over50SourceSamples++;
      actualByAsset[s.asset]?.push(actual);
    }

    for(const {s,actual} of captured){
      const near=makeDepthSample(s,actual,'near');if(near)writer.add(s.venue,s.asset,near);
      if(sec%2===0){const mid=makeDepthSample(s,actual,'mid');if(mid)writer.add(s.venue,s.asset,mid);}
      if(sec%5===0){const far=makeDepthSample(s,actual,'far');if(far)writer.add(s.venue,s.asset,far);const p=makePersistenceSample(s,actual);if(p)writer.add(s.venue,s.asset,p);}
    }

    for(const [asset,xs] of Object.entries(actualByAsset)){
      if(xs.length<2)continue;
      const skew=Math.max(...xs)-Math.min(...xs);
      timingWindow.maxSkewByAsset[asset]=Math.max(timingWindow.maxSkewByAsset[asset],skew);
      timingWindow.maxAssetSkewMs=Math.max(timingWindow.maxAssetSkewMs,skew);
    }
  }

  let next=Math.ceil(Date.now()/1000)*1000;
  const tick=async()=>{
    if(stopping)return;
    const now=Date.now();
    while(next<=now){
      const late=now-next;
      if(late<=50)sampleAt(next);else timingWindow.missedSchedulerSlots++;
      next+=1000;
    }
    const files=await writer.flushClosed(now);
    if(files.length){
      manifest.addFiles(files);await manifest.save();
      volumeBytes=await directoryBytes(DATA_DIR);datasetBytes=await directoryBytes(DATASET_DIR);
      console.log(JSON.stringify({kind:'block-flush',ts:new Date().toISOString(),fileCount:files.length,volumeBytes,datasetBytes,files:files.map(x=>({bytes:x.bytes,sha256:x.sha256,venue:x.venue,asset:x.asset,blockStartMs:x.blockStartMs,records:x.records}))}));
      const need=writer.estimatedNextBatchBytes()+STOP_RESERVE_BYTES;
      if(volumeBytes+need>=CAPACITY_BYTES)return stop('STORAGE_LIMIT',true);
    }
    if(now-collection.data.startMs>=MAX_RUN_MS)return stop('TEN_DAY_LIMIT',true);
    setTimeout(tick,Math.max(1,next-Date.now()));
  };
  tick();

  const telemetry=setInterval(()=>{
    const s=state();const vals=Object.values(s.sources);const timing=timingWindow;timingWindow=newTimingWindow();
    console.log(JSON.stringify({kind:'collector-status',ts:new Date().toISOString(),status:s.status,collectionId:s.collectionId,runId:s.runId,readyCount:vals.filter(x=>x.ready).length,nearReadyCount:vals.filter(x=>x.nearReady).length,totalSources:vals.length,volumeBytes:s.volumeBytes,datasetBytes:s.datasetBytes,estimatedNextBatchBytes:s.estimatedNextBatchBytes,timing,sources:s.sources}));
  },30_000);telemetry.unref();

  async function stop(reason,finalCollection){
    if(stopping)return;stopping=true;status='STOPPING';stopReason=reason;
    for(const f of feeds)f.stop();
    const files=await writer.flushAll();manifest.addFiles(files);
    volumeBytes=await directoryBytes(DATA_DIR);datasetBytes=await directoryBytes(DATASET_DIR);
    const endMs=Date.now();
    await manifest.finish(reason,endMs,volumeBytes);
    await collection.finishSegment(runId,reason,endMs);
    if(finalCollection)await collection.finishCollection(reason,endMs,volumeBytes,datasetBytes);
    status='STOPPED';
    console.log(JSON.stringify({kind:'collector-stop',reason,finalCollection,collectionId:collection.data.collectionId,runId,endIso:new Date(endMs).toISOString(),volumeBytes,datasetBytes}));
  }

  let signalHandling=false;
  const onSignal=signal=>async()=>{
    if(signalHandling)return;signalHandling=true;
    try{await stop(signal,false);}finally{process.exit(0);}
  };
  process.once('SIGTERM',onSignal('SIGTERM'));
  process.once('SIGINT',onSignal('SIGINT'));
}
