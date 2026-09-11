import zlib from 'node:zlib';
import crypto from 'node:crypto';
const MAGIC=Buffer.from('SPDBV1\0\0');
const QUALITY={VALID:1,STALE:2,GAP:3,RESYNC:4,INSUFFICIENT_COVERAGE:5,HISTORY_DEPENDENT:6,MISSING:7,UNKNOWN:8,ALIGNMENT_INVALID:9};
function f32arr(a){const b=Buffer.allocUnsafe(a.length*4);a.forEach((v,i)=>b.writeFloatLE(Number(v)||0,i*4));return b;}
function u8arr(a){return Buffer.from(a.map(v=>v??255));}
function qcode(q){return QUALITY[q]??QUALITY.UNKNOWN;}
export function encodeBlock(meta,records){
  const parts=[];
  for(const r of records){
    if(r.type==='depth'){
      const k=r.kind==='near'?1:r.kind==='mid'?2:3;const h=Buffer.allocUnsafe(1+4+8+4*4+1);h.writeUInt8(k,0);h.writeUInt32LE(r.actualTs-meta.blockStartMs,1);h.writeDoubleLE(r.mid,5);h.writeFloatLE(r.trustedBid,13);h.writeFloatLE(r.trustedAsk,17);h.writeFloatLE(r.observedBid,21);h.writeFloatLE(r.observedAsk,25);h.writeUInt8(qcode(r.quality),29);parts.push(h,f32arr(r.bid),f32arr(r.ask));
    } else if(r.type==='persistence'){
      const h=Buffer.allocUnsafe(6);h.writeUInt8(4,0);h.writeUInt32LE(r.actualTs-meta.blockStartMs,1);h.writeUInt8(qcode(r.quality),5);parts.push(h,u8arr(r.r10b),u8arr(r.r10a),u8arr(r.r30b),u8arr(r.r30a));
    } else if(r.type==='gap'||r.type==='reset'){
      const h=Buffer.allocUnsafe(5);h.writeUInt8(r.type==='gap'?5:6,0);h.writeUInt32LE(r.actualTs-meta.blockStartMs,1);parts.push(h);
    }
  }
  const header=Buffer.from(JSON.stringify({...meta,recordCount:records.length,qualityCodes:QUALITY}),'utf8');const hl=Buffer.allocUnsafe(4);hl.writeUInt32LE(header.length,0);const raw=Buffer.concat([MAGIC,hl,header,...parts]);const sha256=crypto.createHash('sha256').update(raw).digest('hex');const gzip=zlib.gzipSync(raw,{level:6});return {raw,gzip,sha256};
}
