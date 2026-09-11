import crypto from 'node:crypto';
import fsp from 'node:fs/promises';
import path from 'node:path';
import {DATASET_DIR} from '../config.mjs';

const statePath=path.join(DATASET_DIR,'collection-state.json');

export class CollectionState{
  constructor(data){this.data=data;this.path=statePath;}
  static async openOrCreate(){
    await fsp.mkdir(DATASET_DIR,{recursive:true});
    try{
      const data=JSON.parse(await fsp.readFile(statePath,'utf8'));
      if(!data?.collectionId||!data?.startMs||!data?.status)throw new Error('invalid collection-state.json');
      return new CollectionState(data);
    }catch(e){
      if(e?.code!=='ENOENT')throw e;
      const now=Date.now();
      const collectionId=`${new Date(now).toISOString().replace(/[:.]/g,'-')}-${crypto.randomBytes(3).toString('hex')}`;
      const s=new CollectionState({
        format:'SPOT_DEPTH_COLLECTION_V1',collectionId,startMs:now,startIso:new Date(now).toISOString(),
        status:'RUNNING',stopReason:null,segments:[],createdAtMs:now,lastUpdatedMs:now
      });
      await s.save();
      return s;
    }
  }
  async save(){
    this.data.lastUpdatedMs=Date.now();
    const tmp=`${this.path}.tmp`;
    await fsp.writeFile(tmp,JSON.stringify(this.data,null,2));
    await fsp.rename(tmp,this.path);
  }
  async beginSegment(runId,startMs){
    if(this.data.status!=='RUNNING')throw new Error(`collection is ${this.data.status}`);
    for(const seg of this.data.segments){
      if(seg.status==='RUNNING'){
        seg.status='INTERRUPTED';seg.stopReason='PROCESS_CRASH_OR_RESTART';seg.endMs=startMs;seg.endIso=new Date(startMs).toISOString();
      }
    }
    this.data.segments.push({runId,startMs,startIso:new Date(startMs).toISOString(),status:'RUNNING',stopReason:null});
    await this.save();
  }
  async finishSegment(runId,reason,endMs){
    const seg=[...this.data.segments].reverse().find(x=>x.runId===runId);
    if(seg){seg.status='STOPPED';seg.stopReason=reason;seg.endMs=endMs;seg.endIso=new Date(endMs).toISOString();seg.durationMs=endMs-seg.startMs;}
    await this.save();
  }
  async finishCollection(reason,endMs,volumeBytes,datasetBytes){
    this.data.status='STOPPED';this.data.stopReason=reason;this.data.endMs=endMs;this.data.endIso=new Date(endMs).toISOString();
    this.data.durationMs=endMs-this.data.startMs;this.data.volumeBytes=volumeBytes;this.data.datasetBytes=datasetBytes;this.data.segmentCount=this.data.segments.length;
    await this.save();
  }
}
