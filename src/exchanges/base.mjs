import {OrderBook} from '../core/orderbook.mjs';import {PersistenceTracker} from '../core/persistence.mjs';
export class SourceState{
 constructor(venue,asset,pair){this.venue=venue;this.asset=asset;this.pair=pair;this.broadBook=new OrderBook(`${venue}:${asset}:broad`);this.nearBook=this.broadBook;this.persistence=new PersistenceTracker();this.quality='MISSING';this.gaps=0;this.resets=0;this.lastMsgAt=0;this.initialized=false;}
 markSnapshot(book=this.nearBook){book.ready=true;this.persistence.resetFromBook(book,Date.now());this.quality='VALID';this.initialized=true;this.resets++;}
 onLevel(side,p,q,ts=Date.now(),book=this.nearBook){book.update(side,p,q,ts);if(book===this.nearBook)this.persistence.onUpdate(side,p,q,ts);this.lastMsgAt=ts;}
 gap(){this.quality='GAP';this.gaps++;}
}
