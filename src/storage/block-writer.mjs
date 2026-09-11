import fsp from 'node:fs/promises';import path from 'node:path';
import {DATASET_DIR,BLOCK_MS} from '../config.mjs';import {encodeBlock} from './block-codec.mjs';
export class BlockWriter{
  constructor(runId){this.runId=runId;this.base=path.join(DATASET_DIR,runId,'blocks');this.states=new Map();this.recentBatchBytes=[];}
  key(v,a,bs){return `${v}:${a}:${bs}`;}
  async init(){await fsp.mkdir(this.base,{recursive:true});}
  state(v,a,t){const bs=Math.floor(t/BLOCK_MS)*BLOCK_MS;const k=this.key(v,a,bs);let s=this.states.get(k);if(!s){s={venue:v,asset:a,blockStartMs:bs,records:[]};this.states.set(k,s);}return s;}
  add(v,a,r){this.state(v,a,r.actualTs).records.push(r);}
  async flushClosed(now){const current=Math.floor(now/BLOCK_MS)*BLOCK_MS;let bytes=0,files=[];for(const [k,s] of [...this.states]){if(s.blockStartMs>=current)continue;const out=await this.flushState(s);bytes+=out.bytes;files.push(out);this.states.delete(k);}if(bytes){this.recentBatchBytes.push(bytes);if(this.recentBatchBytes.length>12)this.recentBatchBytes.shift();}return files;}
  async flushAll(){let files=[];for(const [k,s] of [...this.states]){if(s.records.length)files.push(await this.flushState(s));this.states.delete(k);}return files;}
  dropOpenBlock(blockStartMs){for(const [k,s] of [...this.states])if(s.blockStartMs===blockStartMs)this.states.delete(k);}
  estimatedNextBatchBytes(){if(!this.recentBatchBytes.length)return 4_000_000;return Math.ceil(Math.max(...this.recentBatchBytes)*1.25);}
  async flushState(s){const meta={version:1,venue:s.venue,asset:s.asset,blockStartMs:s.blockStartMs,blockMs:BLOCK_MS};const {gzip,sha256}=encodeBlock(meta,s.records);const dir=path.join(this.base,s.venue,s.asset);await fsp.mkdir(dir,{recursive:true});const name=`${s.blockStartMs}.spdb.gz`;const file=path.join(dir,name);await fsp.writeFile(file,gzip);await fsp.writeFile(`${file}.sha256`,`${sha256}  ${name}\n`);return {file,bytes:gzip.length,sha256,records:s.records.length,venue:s.venue,asset:s.asset,blockStartMs:s.blockStartMs};}
}
export async function directoryBytes(root){let total=0;async function walk(p){for(const e of await fsp.readdir(p,{withFileTypes:true}).catch(()=>[])){const q=path.join(p,e.name);if(e.isDirectory())await walk(q);else total+=(await fsp.stat(q)).size;}}await walk(root);return total;}
