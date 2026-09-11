export class OrderBook {
  constructor(name) { this.name=name; this.bids=new Map(); this.asks=new Map(); this.ready=false; this.updatedAt=0; }
  clear(){this.bids.clear();this.asks.clear();this.ready=false;}
  replace(bids,asks,ts=Date.now()){
    this.bids=new Map(bids.map(([p,q])=>[Number(p),Number(q)]).filter(([,q])=>q>0));
    this.asks=new Map(asks.map(([p,q])=>[Number(p),Number(q)]).filter(([,q])=>q>0));
    this.ready=true;this.updatedAt=ts;
  }
  update(side,price,qty,ts=Date.now()){
    const m=side==='bid'?this.bids:this.asks; const p=Number(price),q=Number(qty);
    if (!Number.isFinite(p)||!Number.isFinite(q)) return;
    if(q<=0)m.delete(p);else m.set(p,q); this.updatedAt=ts;
  }
  bestBid(){let x=-Infinity;for(const p of this.bids.keys())if(p>x)x=p;return Number.isFinite(x)?x:null;}
  bestAsk(){let x=Infinity;for(const p of this.asks.keys())if(p<x)x=p;return Number.isFinite(x)?x:null;}
  mid(){const b=this.bestBid(),a=this.bestAsk();return b&&a?(b+a)/2:null;}
  coverageBps(mid=this.mid()){
    if(!mid)return {bid:0,ask:0}; let minBid=Infinity,maxAsk=-Infinity;
    for(const p of this.bids.keys())if(p<minBid)minBid=p;
    for(const p of this.asks.keys())if(p>maxAsk)maxAsk=p;
    return {bid:Number.isFinite(minBid)?(mid-minBid)/mid*10000:0, ask:Number.isFinite(maxAsk)?(maxAsk-mid)/mid*10000:0};
  }
  shellQty(bounds,maxBps=2000){
    const mid=this.mid(); if(!mid)return null;
    const bid=new Array(bounds.length-1).fill(0),ask=new Array(bounds.length-1).fill(0);
    const add=(m,side,out)=>{for(const [p,q] of m){const d=side==='bid'?(mid-p)/mid*10000:(p-mid)/mid*10000;if(d<0||d>=maxBps)continue;for(let i=0;i<bounds.length-1;i++){if(d>=bounds[i]&&d<bounds[i+1]){out[i]+=q;break;}}}};
    add(this.bids,'bid',bid);add(this.asks,'ask',ask);return {mid,bid,ask};
  }
}
