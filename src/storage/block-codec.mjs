import zlib from 'node:zlib';
import crypto from 'node:crypto';

const MAGIC=Buffer.from('SPDBV1\0\0');
const QUALITY={VALID:1,STALE:2,GAP:3,RESYNC:4,INSUFFICIENT_COVERAGE:5,HISTORY_DEPENDENT:6,MISSING:7,UNKNOWN:8,ALIGNMENT_INVALID:9};
const QUALITY_BY_CODE=Object.fromEntries(Object.entries(QUALITY).map(([k,v])=>[v,k]));
const DEPTH_COUNT_BY_TYPE={1:7,2:3,3:4};
const KIND_BY_TYPE={1:'near',2:'mid',3:'far'};

function f32arr(a){const b=Buffer.allocUnsafe(a.length*4);a.forEach((v,i)=>b.writeFloatLE(Number(v)||0,i*4));return b;}
function u8arr(a){return Buffer.from(a.map(v=>v??255));}
function qcode(q){return QUALITY[q]??QUALITY.UNKNOWN;}
function relTs(actualTs,blockStartMs){const x=Math.round(actualTs-blockStartMs);if(!Number.isInteger(x)||x<0||x>0xffffffff)throw new Error(`timestamp outside block: ${x}`);return x;}

export function encodeBlock(meta,records){
  const parts=[];
  for(const r of records){
    if(r.type==='depth'){
      const k=r.kind==='near'?1:r.kind==='mid'?2:r.kind==='far'?3:0;
      const expected=DEPTH_COUNT_BY_TYPE[k];
      if(!expected)throw new Error(`unknown depth kind ${r.kind}`);
      if(r.bid.length!==expected||r.ask.length!==expected)throw new Error(`${r.kind} shell length must be ${expected}`);
      const h=Buffer.allocUnsafe(30);
      h.writeUInt8(k,0);h.writeUInt32LE(relTs(r.actualTs,meta.blockStartMs),1);h.writeDoubleLE(r.mid,5);
      h.writeFloatLE(r.trustedBid,13);h.writeFloatLE(r.trustedAsk,17);h.writeFloatLE(r.observedBid,21);h.writeFloatLE(r.observedAsk,25);h.writeUInt8(qcode(r.quality),29);
      parts.push(h,f32arr(r.bid),f32arr(r.ask));
    }else if(r.type==='persistence'){
      for(const a of [r.r10b,r.r10a,r.r30b,r.r30a])if(a.length!==7)throw new Error('persistence shell length must be 7');
      const h=Buffer.allocUnsafe(6);h.writeUInt8(4,0);h.writeUInt32LE(relTs(r.actualTs,meta.blockStartMs),1);h.writeUInt8(qcode(r.quality),5);
      parts.push(h,u8arr(r.r10b),u8arr(r.r10a),u8arr(r.r30b),u8arr(r.r30a));
    }else if(r.type==='gap'||r.type==='reset'){
      const h=Buffer.allocUnsafe(5);h.writeUInt8(r.type==='gap'?5:6,0);h.writeUInt32LE(relTs(r.actualTs,meta.blockStartMs),1);parts.push(h);
    }
  }
  const headerObj={...meta,recordCount:records.length,qualityCodes:QUALITY,depthShellCounts:{near:7,mid:3,far:4},persistenceShellCount:7};
  const header=Buffer.from(JSON.stringify(headerObj),'utf8');const hl=Buffer.allocUnsafe(4);hl.writeUInt32LE(header.length,0);
  const raw=Buffer.concat([MAGIC,hl,header,...parts]);const sha256=crypto.createHash('sha256').update(raw).digest('hex');
  const gzip=zlib.gzipSync(raw,{level:6});return {raw,gzip,sha256};
}

export function decodeBlock(input,{gzip=true}={}){
  const raw=gzip?zlib.gunzipSync(input):input;
  if(raw.length<12||!raw.subarray(0,8).equals(MAGIC))throw new Error('invalid SPDBV1 magic');
  const headerLength=raw.readUInt32LE(8);const headerEnd=12+headerLength;
  if(headerEnd>raw.length)throw new Error('truncated header');
  const meta=JSON.parse(raw.toString('utf8',12,headerEnd));
  let o=headerEnd;const records=[];
  const need=n=>{if(o+n>raw.length)throw new Error('truncated record');};
  const readF32=n=>{need(n*4);const a=[];for(let i=0;i<n;i++)a.push(raw.readFloatLE(o+i*4));o+=n*4;return a;};
  while(o<raw.length){
    const type=raw.readUInt8(o);
    if(type>=1&&type<=3){
      const n=DEPTH_COUNT_BY_TYPE[type];need(30+2*n*4);
      const actualTs=meta.blockStartMs+raw.readUInt32LE(o+1);const mid=raw.readDoubleLE(o+5);
      const trustedBid=raw.readFloatLE(o+13),trustedAsk=raw.readFloatLE(o+17),observedBid=raw.readFloatLE(o+21),observedAsk=raw.readFloatLE(o+25),quality=QUALITY_BY_CODE[raw.readUInt8(o+29)]||'UNKNOWN';
      o+=30;const bid=readF32(n),ask=readF32(n);
      records.push({type:'depth',kind:KIND_BY_TYPE[type],actualTs,mid,bid,ask,trustedBid,trustedAsk,observedBid,observedAsk,quality});
    }else if(type===4){
      need(34);const actualTs=meta.blockStartMs+raw.readUInt32LE(o+1),quality=QUALITY_BY_CODE[raw.readUInt8(o+5)]||'UNKNOWN';o+=6;
      const readU8=()=>{need(7);const a=[...raw.subarray(o,o+7)];o+=7;return a;};
      records.push({type:'persistence',actualTs,quality,r10b:readU8(),r10a:readU8(),r30b:readU8(),r30a:readU8()});
    }else if(type===5||type===6){
      need(5);const actualTs=meta.blockStartMs+raw.readUInt32LE(o+1);o+=5;records.push({type:type===5?'gap':'reset',actualTs});
    }else throw new Error(`unknown record type ${type} at ${o}`);
  }
  if(meta.recordCount!==records.length)throw new Error(`record count mismatch ${records.length} != ${meta.recordCount}`);
  const sha256=crypto.createHash('sha256').update(raw).digest('hex');
  return {meta,records,raw,sha256};
}

export {QUALITY};
