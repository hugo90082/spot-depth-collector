export class PersistenceTracker {
  constructor(){this.bid=new Map();this.ask=new Map();}
  resetFromBook(book,ts=Date.now()){
    this.bid.clear();this.ask.clear();
    for(const [p,q] of book.bids)this.bid.set(p,{since:ts,q,history:[[ts,q]]});
    for(const [p,q] of book.asks)this.ask.set(p,{since:ts,q,history:[[ts,q]]});
  }
  onUpdate(side,price,qty,ts=Date.now()){
    const map=side==='bid'?this.bid:this.ask; const p=Number(price),q=Number(qty); const old=map.get(p);
    if(q<=0){map.delete(p);return;}
    if(!old){map.set(p,{since:ts,q,history:[[ts,q]]});return;}
    old.q=q;old.history.push([ts,q]);
    const cut=ts-35_000;
    while(old.history.length>1 && old.history[1][0] <= cut) old.history.shift();
  }
  persistentQty(side,price,now,windowMs){
    const map=side==='bid'?this.bid:this.ask; const s=map.get(price); const start=now-windowMs;
    if(!s||s.since>start)return 0;
    let currentAtStart=s.history[0]?.[1] ?? s.q;
    for(const [t,q] of s.history){if(t<=start)currentAtStart=q;else break;}
    let min=currentAtStart;
    for(const [t,q] of s.history){if(t>start&&t<=now&&q<min)min=q;}
    return Math.max(0,min);
  }
  shellRatios(book,bounds,now,windowMs){
    const mid=book.mid();if(!mid)return null;const out={bid:[],ask:[]};
    for(const side of ['bid','ask']){
      const m=side==='bid'?book.bids:book.asks;
      for(let i=0;i<7;i++){let q=0,persist=0;for(const [price,qty] of m){const d=side==='bid'?(mid-price)/mid*10000:(price-mid)/mid*10000;if(d>=bounds[i]&&d<bounds[i+1]){q+=qty;persist+=this.persistentQty(side,price,now,windowMs);}}out[side][i]=q>0?persist/q:null;}
    }
    return out;
  }
}
