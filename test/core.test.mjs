import test from 'node:test';
import assert from 'node:assert/strict';
import {OrderBook} from '../src/core/orderbook.mjs';
import {PersistenceTracker} from '../src/core/persistence.mjs';
import {makeDepthSample} from '../src/core/sample.mjs';
import {encodeBlock,decodeBlock} from '../src/storage/block-codec.mjs';
import {BlockWriter} from '../src/storage/block-writer.mjs';

test('shell aggregation uses left-closed/right-open boundaries',()=>{
  const b=new OrderBook('x');b.replace([[99.5,2],[98,3]],[[100.5,4],[102,5]]);
  const s=b.shellQty([0,100,300]);assert.equal(s.mid,100);assert.equal(s.bid[0],2);assert.equal(s.ask[0],4);assert.equal(s.bid[1],3);assert.equal(s.ask[1],5);
});

test('persistence reset on disappearance',()=>{
  const b=new OrderBook('x');b.replace([[99,2]],[[101,1]],0);const p=new PersistenceTracker();p.resetFromBook(b,0);
  assert.equal(p.persistentQty('bid',99,11000,10000),2);p.onUpdate('bid',99,0,12000);p.onUpdate('bid',99,5,13000);assert.equal(p.persistentQty('bid',99,20000,10000),0);
});

test('persistence keeps quantity at window start when level increased',()=>{
  const b=new OrderBook('x');b.replace([[99,1]],[[101,1]],0);const p=new PersistenceTracker();p.resetFromBook(b,0);p.onUpdate('bid',99,5,5000);
  assert.equal(p.persistentQty('bid',99,12000,10000),1);
});

test('depth samples use locked 7/3/4 shell counts',()=>{
  const b=new OrderBook('x');
  const bids=[],asks=[];for(let i=1;i<=200;i++){bids.push([100-i*0.01,1]);asks.push([100+i*0.01,1]);}b.replace(bids,asks,1000);
  const source={broadBook:b,nearBook:b,quality:'VALID'};
  assert.equal(makeDepthSample(source,1100,'near').bid.length,7);
  assert.equal(makeDepthSample(source,1100,'mid').bid.length,3);
  assert.equal(makeDepthSample(source,1100,'far').bid.length,4);
});

test('block codec round-trips near/mid/far binary payload and checksum',()=>{
  const meta={version:1,venue:'x',asset:'BTC',blockStartMs:1000,blockMs:300000};
  const base={type:'depth',actualTs:1100,mid:100,trustedBid:100,trustedAsk:100,observedBid:100,observedAsk:100,quality:'VALID'};
  const records=[
    {...base,kind:'near',bid:new Array(7).fill(1),ask:new Array(7).fill(2)},
    {...base,actualTs:2100,kind:'mid',bid:new Array(3).fill(3),ask:new Array(3).fill(4)},
    {...base,actualTs:3100,kind:'far',bid:new Array(4).fill(5),ask:new Array(4).fill(6)},
    {type:'persistence',actualTs:5100,quality:'VALID',r10b:new Array(7).fill(254),r10a:new Array(7).fill(0),r30b:new Array(7).fill(127),r30a:new Array(7).fill(255)},
    {type:'gap',actualTs:6100},{type:'reset',actualTs:7100}
  ];
  const e=encodeBlock(meta,records);const d=decodeBlock(e.gzip);
  assert.equal(d.sha256,e.sha256);assert.equal(d.records.length,records.length);
  assert.equal(d.records[0].bid.length,7);assert.equal(d.records[1].bid.length,3);assert.equal(d.records[2].bid.length,4);
  assert.deepEqual(d.records[3].r10b,new Array(7).fill(254));assert.equal(d.records[4].type,'gap');assert.equal(d.records[5].type,'reset');
});

test('codec rejects timestamp outside its physical 5-minute block',()=>{
  const meta={version:1,venue:'x',asset:'BTC',blockStartMs:300000,blockMs:300000};
  const r={type:'gap',actualTs:299999};
  assert.throws(()=>encodeBlock(meta,[r]),/timestamp outside block/);
});

test('block writer retains adjacent blocks simultaneously',()=>{
  const w=new BlockWriter('x');w.add('v','BTC',{type:'reset',actualTs:299999});w.add('v','BTC',{type:'reset',actualTs:300000});assert.equal(w.states.size,2);
});
