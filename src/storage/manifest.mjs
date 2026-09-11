import fsp from 'node:fs/promises';import path from 'node:path';import {DATASET_DIR} from '../config.mjs';
export class Manifest{
 constructor(runId,startMs){this.runId=runId;this.root=path.join(DATASET_DIR,runId);this.path=path.join(this.root,'dataset-manifest.json');this.data={format:'SPOT_DEPTH_DATASET_V1',runId,startMs,startIso:new Date(startMs).toISOString(),status:'RUNNING',stopReason:null,files:[],gapCount:0,resetCount:0};}
 async init(){await fsp.mkdir(this.root,{recursive:true});await this.save();}
 addFiles(xs){for(const x of xs)this.data.files.push({path:path.relative(this.root,x.file),bytes:x.bytes,sha256:x.sha256,records:x.records,venue:x.venue,asset:x.asset,blockStartMs:x.blockStartMs});}
 event(type){if(type==='gap')this.data.gapCount++;if(type==='reset')this.data.resetCount++;}
 async save(){await fsp.writeFile(this.path,JSON.stringify(this.data,null,2));}
 async finish(reason,endMs,totalBytes){this.data.status='STOPPED';this.data.stopReason=reason;this.data.endMs=endMs;this.data.endIso=new Date(endMs).toISOString();this.data.durationMs=endMs-this.data.startMs;this.data.totalBytes=totalBytes;this.data.fileCount=this.data.files.length;await this.save();}
}
