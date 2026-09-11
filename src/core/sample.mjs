import {MIXED14} from '../config.mjs';
export function quantizeRatio(r){if(r==null||!Number.isFinite(r))return 255;return Math.max(0,Math.min(254,Math.round(r*254)));}
export function makeDepthSample(source,actualTs,kind){
  const book=kind==='near'&&source.nearBook?source.nearBook:source.broadBook;
  const s=book?.shellQty(MIXED14); if(!s)return null;
  const cov=book.coverageBps(s.mid); const trusted=source.trustedCoverage?.(kind,s.mid,cov)||cov;
  const quality=source.qualityFor?.(kind) ?? source.quality ?? 'VALID';
  return {type:'depth',kind,actualTs,mid:s.mid,bid:s.bid,ask:s.ask,trustedBid:trusted.bid,trustedAsk:trusted.ask,observedBid:cov.bid,observedAsk:cov.ask,quality};
}
export function makePersistenceSample(source,actualTs){
  const book=source.nearBook||source.broadBook;if(!book||!source.persistence)return null;
  const r10=source.persistence.shellRatios(book,MIXED14,actualTs,10_000);const r30=source.persistence.shellRatios(book,MIXED14,actualTs,30_000);if(!r10||!r30)return null;
  const quality=source.qualityFor?.('near') ?? source.quality ?? 'VALID';
  return {type:'persistence',actualTs,r10b:r10.bid.map(quantizeRatio),r10a:r10.ask.map(quantizeRatio),r30b:r30.bid.map(quantizeRatio),r30a:r30.ask.map(quantizeRatio),quality};
}
