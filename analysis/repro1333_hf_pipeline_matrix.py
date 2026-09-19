#!/usr/bin/env python3
import argparse, csv, gzip, json, math, os, struct, sys, time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

MAGIC = b"SPDBV1\x00\x00"
BOUNDS = [0,5,10,25,50,100,200,300,500,750,1000,1250,1500,1750,2000]
RANGES = [f"{BOUNDS[i]}-{BOUNDS[i+1]}" for i in range(14)]
TYPE_INFO = {1:(0,7), 2:(7,10), 3:(10,14)}
HORIZONS = {"BTC":600, "ETH":60, "SOL":180}
ASSETS = ["BTC","ETH","SOL"]
VALID_QUALITY = 1
CHILD_USD = 10.0
FEE_BPS = 3.87
FEE_RATE = FEE_BPS / 10000.0
MIN_HOLD_S = 300
CAP_USD = 30000.0

def iso_ms(ms):
    return datetime.fromtimestamp(ms/1000, tz=timezone.utc).isoformat().replace("+00:00","Z")

def quantile_linear(values, q):
    xs = sorted(float(x) for x in values if x is not None and math.isfinite(float(x)))
    n = len(xs)
    if n == 0:
        return None
    if n == 1:
        return xs[0]
    h = (n - 1) * q
    lo = int(math.floor(h))
    hi = int(math.ceil(h))
    if lo == hi:
        return xs[lo]
    w = h - lo
    return xs[lo] * (1.0 - w) + xs[hi] * w

def pearson(xs, ys):
    n = len(xs)
    if n < 2:
        return None
    mx = sum(xs)/n
    my = sum(ys)/n
    vx = sum((x-mx)*(x-mx) for x in xs)
    vy = sum((y-my)*(y-my) for y in ys)
    if vx <= 0 or vy <= 0:
        return None
    cov = sum((x-mx)*(y-my) for x,y in zip(xs,ys))
    return cov / math.sqrt(vx*vy)

def sign(x):
    if x > 0: return 1
    if x < 0: return -1
    return 0

def discover_files(root, asset):
    rows = []
    for run in sorted(root.iterdir()):
        d = run / "blocks" / "binance" / asset
        if not d.is_dir():
            continue
        for p in d.glob("*.spdb.gz"):
            head = p.name.split(".",1)[0]
            if not head.isdigit():
                continue
            rows.append((int(head), run.name, p))
    rows.sort(key=lambda x:(x[0],x[1],str(x[2])))
    return rows

def iter_block_records(path):
    with gzip.open(path, "rb") as f:
        raw = f.read()
    if len(raw) < 12 or raw[:8] != MAGIC:
        raise ValueError(f"invalid SPDBV1 magic: {path}")
    hlen = struct.unpack_from("<I", raw, 8)[0]
    hend = 12 + hlen
    if hend > len(raw):
        raise ValueError(f"truncated header: {path}")
    meta = json.loads(raw[12:hend].decode("utf-8"))
    bstart = int(meta["blockStartMs"])
    o = hend
    seen = 0
    L = len(raw)
    while o < L:
        typ = raw[o]
        if typ in (1,2,3):
            lo,hi = TYPE_INFO[typ]
            n = hi-lo
            rec_len = 30 + 8*n
            if o + rec_len > L:
                raise ValueError(f"truncated depth record {path} @{o}")
            dt = struct.unpack_from("<I", raw, o+1)[0]
            actual = bstart + dt
            mid = struct.unpack_from("<d", raw, o+5)[0]
            trusted_bid = struct.unpack_from("<f", raw, o+13)[0]
            trusted_ask = struct.unpack_from("<f", raw, o+17)[0]
            observed_bid = struct.unpack_from("<f", raw, o+21)[0]
            observed_ask = struct.unpack_from("<f", raw, o+25)[0]
            quality = raw[o+29]
            p = o + 30
            bids = struct.unpack_from("<" + "f"*n, raw, p)
            p += 4*n
            asks = struct.unpack_from("<" + "f"*n, raw, p)
            o += rec_len
            seen += 1
            yield ("depth", actual, typ, mid, trusted_bid, trusted_ask, observed_bid, observed_ask, quality, bids, asks)
        elif typ == 4:
            if o + 34 > L:
                raise ValueError(f"truncated persistence record {path} @{o}")
            actual = bstart + struct.unpack_from("<I", raw, o+1)[0]
            o += 34
            seen += 1
            yield ("persistence", actual)
        elif typ in (5,6):
            if o + 5 > L:
                raise ValueError(f"truncated event record {path} @{o}")
            actual = bstart + struct.unpack_from("<I", raw, o+1)[0]
            o += 5
            seen += 1
            yield ("gap" if typ==5 else "reset", actual)
        else:
            raise ValueError(f"unknown record type {typ}: {path} @{o}")
    rc = meta.get("recordCount")
    if rc is not None and int(rc) != seen:
        raise ValueError(f"record count mismatch {seen}!={rc}: {path}")

def finalize_minute(minute_t, price_last, sums, counts, sample_count):
    if minute_t is None or price_last is None or not math.isfinite(price_last) or price_last <= 0:
        return None
    imb = []
    for s,c in zip(sums,counts):
        imb.append(s/c if c else None)
    return {
        "t": minute_t,
        "price": price_last,
        "imb": imb,
        "counts": list(counts),
        "samples": sample_count,
    }

def run_strategy(asset, signals, cap_usd):
    cap_children = None if cap_usd is None else int(round(cap_usd/CHILD_USD))
    children = deque()
    direction = 0
    dir_start = None
    last_add_t = None
    last_exit_t = None
    sum_inv = 0.0
    realized = 0.0
    fee_per_child = CHILD_USD * FEE_RATE
    blocked = 0
    entries = exits = 0
    long_entries = short_entries = 0
    nonzero_signal_bars = 0
    ignored_minhold = 0
    blocked_add_cadence = 0
    blocked_exit_cadence = 0
    max_open = max_long = max_short = 0.0
    exposure_sum = 0.0
    bars_long=bars_short=bars_flat=0
    curve=[]
    events=[]
    peak=0.0
    max_dd=0.0

    def add(n, dirn, t, price, reason):
        nonlocal realized,sum_inv,entries,long_entries,short_entries,blocked,direction,dir_start,last_add_t
        if n <= 0: return 0
        room = n if cap_children is None else max(0, cap_children-len(children))
        actual = min(n,room)
        blocked_now = n-actual
        blocked += blocked_now
        if actual:
            if direction == 0:
                direction = dirn
                dir_start = t
            for _ in range(actual):
                children.append((t,price))
                sum_inv += 1.0/price
            realized -= actual*fee_per_child
            entries += actual
            last_add_t = t
            if dirn>0: long_entries += actual
            else: short_entries += actual
            events.append((t,reason,dirn,actual,price,len(children),realized,blocked_now))
        elif blocked_now:
            events.append((t,"BLOCKED_CAP",dirn,0,price,len(children),realized,blocked_now))
        return actual

    def close(n,t,price,reason):
        nonlocal realized,sum_inv,exits,direction,dir_start,last_exit_t,last_add_t
        actual=min(n,len(children))
        if actual<=0:return 0
        dirn=direction
        pnl=0.0
        for _ in range(actual):
            et,ep=children.popleft()
            sum_inv -= 1.0/ep
            if dirn>0:
                pnl += CHILD_USD*(price/ep - 1.0)
            else:
                pnl += CHILD_USD*(1.0 - price/ep)
        realized += pnl - actual*fee_per_child
        exits += actual
        last_exit_t = t
        events.append((t,reason,dirn,-actual,price,len(children),realized,0))
        if not children:
            direction=0
            dir_start=None
            last_add_t=None
            sum_inv=0.0
        return actual

    for s in signals:
        t,p,net=s["t"],s["price"],s["net"]
        if net:
            nonzero_signal_bars += 1

        if direction == 0:
            # Initial opening remains responsive to each 30s signal.
            if net>0:
                add(abs(net),1,t,p,"OPEN_LONG")
                last_exit_t=None
            elif net<0:
                add(abs(net),-1,t,p,"OPEN_SHORT")
                last_exit_t=None

        elif net == 0:
            pass

        elif sign(net) == direction:
            # Same-direction adding is throttled to one action per >=60s.
            if last_add_t is None or (t-last_add_t) >= 60_000:
                add(abs(net),direction,t,p,"ADD_60S")
            else:
                blocked_add_cadence += 1

        else:
            # Opposite votes: first respect the 300s direction hold, then throttle exits to >=60s.
            if dir_start is not None and (t-dir_start) < MIN_HOLD_S*1000:
                ignored_minhold += 1
            else:
                req=abs(net)
                c=close(req,t,p,"PARTIAL_CLOSE_30S")
                rem=req-c
                if rem>0 and direction==0:
                    nd=sign(net)
                    add(rem,nd,t,p,"REVERSE_OPEN_AFTER_60S_EXIT")
                    # New direction starts a fresh exit cadence and 300s hold clock.
                    last_exit_t=None

        n=len(children)
        open_usd=n*CHILD_USD
        if direction>0:
            unreal=CHILD_USD*p*sum_inv - CHILD_USD*n
        elif direction<0:
            unreal=CHILD_USD*n - CHILD_USD*p*sum_inv
        else:
            unreal=0.0
        equity=realized+unreal
        peak=max(peak,equity)
        dd=equity-peak
        max_dd=min(max_dd,dd)
        max_open=max(max_open,open_usd)
        if direction>0:max_long=max(max_long,open_usd)
        if direction<0:max_short=max(max_short,open_usd)
        exposure_sum += open_usd
        if direction>0:bars_long+=1
        elif direction<0:bars_short+=1
        else:bars_flat+=1
        curve.append((t,equity,realized,unreal,open_usd,direction,n,s["bull"],s["bear"],net,p))

    if curve:
        final=curve[-1]
        final_pnl=final[1]; final_realized=final[2]; final_unreal=final[3]; final_open=final[4]
        start_ms=curve[0][0]; end_ms=curve[-1][0]
    else:
        final_pnl=final_realized=final_unreal=final_open=0.0
        start_ms=end_ms=None
    summary={
        "asset":asset,
        "cap_mode":"uncapped" if cap_usd is None else f"cap{int(cap_usd)}",
        "cap_usd":cap_usd,
        "final_pnl":final_pnl,
        "realized_pnl":final_realized,
        "unrealized_pnl":final_unreal,
        "max_open_usd":max_open,
        "max_drawdown":max_dd,
        "final_open_usd":final_open,
        "blocked_child_count":blocked,
        "child_entries":entries,
        "child_exits":exits,
        "long_child_entries":long_entries,
        "short_child_entries":short_entries,
        "nonzero_signal_bars":nonzero_signal_bars,
        "ignored_reverse_minhold_bars":ignored_minhold,
        "blocked_add_cadence_bars":blocked_add_cadence,
        "blocked_exit_cadence_bars":blocked_exit_cadence,
        "max_long_usd":max_long,
        "max_short_usd":max_short,
        "avg_abs_open_usd":exposure_sum/len(curve) if curve else 0.0,
        "bars_long":bars_long,
        "bars_short":bars_short,
        "bars_flat":bars_flat,
        "start_ms":start_ms,
        "end_ms":end_ms,
    }
    return summary,curve,events

def combine_curves(curves_by_asset):
    all_ts=sorted(set().union(*[set(p[0] for p in c) for c in curves_by_asset.values()]))
    maps={a:{p[0]:p for p in c} for a,c in curves_by_asset.items()}
    last={a:(0.0,0.0) for a in curves_by_asset}
    out=[]
    peak=0.0
    max_dd=0.0
    max_open=0.0
    for t in all_ts:
        for a,m in maps.items():
            if t in m:
                p=m[t]
                last[a]=(p[1],p[4])
        eq=sum(v[0] for v in last.values())
        op=sum(v[1] for v in last.values())
        peak=max(peak,eq)
        dd=eq-peak
        max_dd=min(max_dd,dd)
        max_open=max(max_open,op)
        out.append((t,eq,op,dd))
    return out,{"final_pnl":out[-1][1] if out else 0.0,"max_open_usd":max_open,"max_drawdown":max_dd}

def write_csv(path, rows, fields=None):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open("w",newline="",encoding="utf-8") as f:
        if fields is None:
            if not rows:return
            fields=list(rows[0].keys())
        w=csv.DictWriter(f,fieldnames=fields)
        w.writeheader()
        for r in rows:w.writerow(r)

def write_diag(path, rows):
    write_csv(path,rows,[
        "asset","shell_idx","shell","upper_bps","bar_valid_count","mean_samples_per_valid_bar",
        "pos_threshold","neg_threshold","N","hit_rate","corr","selected"
    ])

def write_signals(path, signals):
    with path.open("w",newline="",encoding="utf-8") as f:
        w=csv.writer(f);w.writerow(["ts_ms","ts_utc","price","bull_count","bear_count","net_count"])
        for s in signals:w.writerow([s["t"],iso_ms(s["t"]),s["price"],s["bull"],s["bear"],s["net"]])

def write_curve(path, curve):
    with path.open("w",newline="",encoding="utf-8") as f:
        w=csv.writer(f);w.writerow(["ts_ms","ts_utc","equity","realized","unrealized","open_usd","direction","child_count","bull","bear","net","price"])
        for p in curve:w.writerow([p[0],iso_ms(p[0]),*p[1:]])

def write_events(path,events):
    with path.open("w",newline="",encoding="utf-8") as f:
        w=csv.writer(f);w.writerow(["ts_ms","ts_utc","action","direction","child_delta","price","open_children","realized_after","blocked_now"])
        for e in events:w.writerow([e[0],iso_ms(e[0]),*e[1:]])

def write_combined_curve(path,curve):
    with path.open("w",newline="",encoding="utf-8") as f:
        w=csv.writer(f);w.writerow(["ts_ms","ts_utc","equity","open_usd","drawdown"])
        for p in curve:w.writerow([p[0],iso_ms(p[0]),p[1],p[2],p[3]])

def svg_equity(path,title,series):
    pts=[]
    for name,curve in series.items():
        for p in curve:
            pts.append((p[0],p[1]))
    if not pts:return
    xmin=min(x for x,y in pts); xmax=max(x for x,y in pts)
    ymin=min(min(y for x,y in pts),0.0); ymax=max(max(y for x,y in pts),0.0)
    if xmax==xmin:xmax=xmin+1
    if ymax==ymin:ymax=ymin+1
    pad=(ymax-ymin)*0.08
    ymin-=pad;ymax+=pad
    W,H=1200,520; L,R,T,B=70,25,45,55
    def X(x):return L+(x-xmin)/(xmax-xmin)*(W-L-R)
    def Y(y):return T+(ymax-y)/(ymax-ymin)*(H-T-B)
    parts=[f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">',
           '<rect width="100%" height="100%" fill="white"/>',
           f'<text x="{L}" y="26" font-family="sans-serif" font-size="18">{title}</text>',
           f'<line x1="{L}" y1="{Y(0)}" x2="{W-R}" y2="{Y(0)}" stroke="black" stroke-width="0.7"/>']
    dashes=["","8,5","2,4","12,4,2,4"]
    for k,(name,curve) in enumerate(series.items()):
        coords=" ".join(f"{X(p[0]):.1f},{Y(p[1]):.1f}" for p in curve)
        dash=dashes[k%len(dashes)]
        dashattr=f' stroke-dasharray="{dash}"' if dash else ""
        parts.append(f'<polyline points="{coords}" fill="none" stroke="black" stroke-width="{1.3+k*0.2}"{dashattr}/>')
        parts.append(f'<text x="{L+160*k}" y="{H-18}" font-family="sans-serif" font-size="13">{name} {"(dashed)" if dash else "(solid)"}</text>')
    parts.append(f'<text x="5" y="{T+10}" font-family="sans-serif" font-size="11">{ymax:.2f}</text>')
    parts.append(f'<text x="5" y="{H-B}" font-family="sans-serif" font-size="11">{ymin:.2f}</text>')
    parts.append('</svg>')
    path.write_text("\n".join(parts),encoding="utf-8")

def svg_vote(path,title,signals):
    if not signals:return
    W,H=1200,650;L,R,T,B=70,25,45,45
    midY=360
    xmin=signals[0]["t"];xmax=signals[-1]["t"]
    pmin=min(s["price"] for s in signals);pmax=max(s["price"] for s in signals)
    cmax=max([1]+[max(s["bull"],s["bear"],abs(s["net"])) for s in signals])
    if xmax==xmin:xmax=xmin+1
    if pmax==pmin:pmax=pmin+1
    def X(x):return L+(x-xmin)/(xmax-xmin)*(W-L-R)
    def YP(p):return T+(pmax-p)/(pmax-pmin)*(midY-T-30)
    def YC(c):return midY+35+(cmax-c)/(2*cmax)*(H-midY-B-45)
    price_pts=" ".join(f"{X(s['t']):.1f},{YP(s['price']):.1f}" for s in signals)
    bull_pts=" ".join(f"{X(s['t']):.1f},{YC(s['bull']):.1f}" for s in signals)
    bear_pts=" ".join(f"{X(s['t']):.1f},{YC(-s['bear']):.1f}" for s in signals)
    net_pts=" ".join(f"{X(s['t']):.1f},{YC(s['net']):.1f}" for s in signals)
    parts=[f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">',
           '<rect width="100%" height="100%" fill="white"/>',
           f'<text x="{L}" y="26" font-family="sans-serif" font-size="18">{title}</text>',
           f'<polyline points="{price_pts}" fill="none" stroke="black" stroke-width="1.2"/>',
           f'<text x="{L}" y="{midY-8}" font-family="sans-serif" font-size="12">Price</text>',
           f'<line x1="{L}" y1="{YC(0)}" x2="{W-R}" y2="{YC(0)}" stroke="black" stroke-width="0.7"/>',
           f'<polyline points="{bull_pts}" fill="none" stroke="black" stroke-width="1.1"/>',
           f'<polyline points="{bear_pts}" fill="none" stroke="black" stroke-width="1.1" stroke-dasharray="8,5"/>',
           f'<polyline points="{net_pts}" fill="none" stroke="black" stroke-width="1.5" stroke-dasharray="2,4"/>',
           f'<text x="{L}" y="{H-12}" font-family="sans-serif" font-size="12">bull + / bear - / net dotted</text>',
           '</svg>']
    path.write_text("\n".join(parts),encoding="utf-8")

def progress(asset,idx,total,block_ts):
    if idx==1 or idx%100==0 or idx==total:
        print(json.dumps({"kind":"progress","asset":asset,"file":idx,"total":total,"block_ts":block_ts}),flush=True)


from array import array
import bisect

def build_regular_1s(root, asset, clear_on_gap=False):
    files=discover_files(root,asset)
    if not files: raise RuntimeError("no files "+asset)
    state=[None]*14
    ready=False
    current_run=None
    sec_rows={}
    pending_ts=None
    pending=[]
    gaps=resets=records=0

    def apply_group(ts, evs):
        nonlocal ready,gaps,resets,state
        last_price=None
        saw_depth=False
        for rec in evs:
            kind=rec[0]
            if kind=="gap":
                gaps+=1
                if clear_on_gap:
                    ready=False; state=[None]*14
                continue
            if kind=="reset":
                resets+=1
                if clear_on_gap:
                    ready=True; state=[None]*14
                continue
            if kind!="depth": continue
            _,actual,typ,mid,tb,ta,obsa,obsb,quality,bids,asks=rec
            lo,hi=TYPE_INFO[typ]
            # Legacy exploratory candidate: no trusted-coverage gate.
            # Accept any finite positive stored shell from depth records.
            if quality not in (1,6):
                continue
            for j,i in enumerate(range(lo,hi)):
                b=float(bids[j]); a=float(asks[j])
                if math.isfinite(b) and math.isfinite(a) and (b+a)>0:
                    state[i]=(b,a)
            if math.isfinite(mid) and mid>0:
                last_price=float(mid); saw_depth=True
        if not saw_depth or last_price is None: return
        sec=(ts//1000)*1000
        vals=[]
        for ba in state:
            if ba is None: vals.append(None)
            else:
                b,a=ba; vals.append((b-a)/(b+a) if (b+a)>0 else None)
        sec_rows[sec]=(last_price,tuple(vals))

    for block_ts,run_name,path in files:
        if current_run!=run_name:
            if pending:
                apply_group(pending_ts,pending)
                pending_ts=None;pending=[]
            current_run=run_name
            # Old stitched-wide candidate: preserve forward-filled state across run boundary.
            ready=True
        for rec in iter_block_records(path):
            records+=1
            ts=rec[1]
            if pending_ts is None:
                pending_ts=ts;pending=[rec]
            elif ts==pending_ts:
                pending.append(rec)
            else:
                apply_group(pending_ts,pending)
                pending_ts=ts;pending=[rec]
    if pending: apply_group(pending_ts,pending)
    if not sec_rows: raise RuntimeError("no rows "+asset)
    secs=sorted(sec_rows)
    start=secs[0];end=secs[-1]
    idx={t:sec_rows[t] for t in secs}
    ts_arr=array('q')
    price=array('d')
    cols=[array('d') for _ in range(14)]
    last_p=None; last_x=[float('nan')]*14
    for t in range(start,end+1000,1000):
        row=idx.get(t)
        if row is not None:
            last_p=row[0]
            for i,x in enumerate(row[1]):
                if x is not None and math.isfinite(x): last_x[i]=x
        if last_p is None: continue
        ts_arr.append(t);price.append(last_p)
        for i in range(14): cols[i].append(last_x[i])
    return ts_arr,price,cols,{"seconds":len(ts_arr),"start":ts_arr[0],"end":ts_arr[-1],"files":len(files),"records":records,"gaps":gaps,"resets":resets}

def qfinite(col, positive=None):
    vals=[]
    for x in col:
        if not math.isfinite(x): continue
        if positive is True and x<=0: continue
        if positive is False and x>=0: continue
        vals.append(x)
    return vals

def hf_diagnostics(ts,price,cols,horizon_s):
    out=[]
    nrow=len(ts)
    h=horizon_s
    for i,col in enumerate(cols):
        pos=qfinite(col,True);neg=qfinite(col,False)
        pt=quantile_linear(pos,.90);nt=quantile_linear(neg,.10)
        hit=0;n=0;xs=[];ys=[]
        for j in range(0,nrow-h):
            x=col[j]
            if not math.isfinite(x) or pt is None or nt is None: continue
            if not (x>=pt or x<=nt): continue
            p0=price[j];p1=price[j+h]
            r=p1/p0-1.0
            if r==0: continue
            n+=1;hit+=(sign(x)==sign(r));xs.append(x);ys.append(r)
        hr=hit/n if n else None
        out.append({"shell_idx":i,"shell":RANGES[i],"pos_threshold":pt,"neg_threshold":nt,
                    "N":n,"hit_rate":hr,"corr":pearson(xs,ys),
                    "selected":bool(hr is not None and hr>.5)})
    return out

def build_30s_bars(ts,price,cols,mode):
    # regular 1s grid; group exact 30 observations.
    bars=[]
    n=len(ts)
    k=0
    while k<n:
        bucket=(ts[k]//30000)*30000
        j=k
        while j<n and (ts[j]//30000)*30000==bucket: j+=1
        vals=[]
        for i in range(14):
            finite=[cols[i][z] for z in range(k,j) if math.isfinite(cols[i][z])]
            if not finite: vals.append(None)
            elif mode=="last": vals.append(finite[-1])
            else: vals.append(sum(finite)/len(finite))
        bars.append({"t":bucket,"price":price[j-1],"imb":vals})
        k=j
    return bars

def thresholds_from_bars(bars):
    out=[]
    for i in range(14):
        vals=[b["imb"][i] for b in bars if b["imb"][i] is not None and math.isfinite(b["imb"][i])]
        out.append((quantile_linear([x for x in vals if x>0],.90),
                    quantile_linear([x for x in vals if x<0],.10)))
    return out

def signals_from_hf_selected(bars,hfdiag,signal_threshold_source):
    selected=[d for d in hfdiag if d["selected"]]
    if signal_threshold_source=="hf":
        th={d["shell_idx"]:(d["pos_threshold"],d["neg_threshold"]) for d in selected}
    else:
        bt=thresholds_from_bars(bars)
        th={d["shell_idx"]:bt[d["shell_idx"]] for d in selected}
    sig=[]
    for b in bars:
        bull=bear=0
        for d in selected:
            i=d["shell_idx"]; x=b["imb"][i]
            if x is None: continue
            pt,nt=th[i]
            if pt is not None and x>=pt: bull+=1
            elif nt is not None and x<=nt: bear+=1
        sig.append({"t":b["t"],"price":b["price"],"bull":bull,"bear":bear,"net":bull-bear})
    return sig,[d["shell_idx"] for d in selected]

def main():
    ap=argparse.ArgumentParser();ap.add_argument("--root",default="/data/spot-depth-production-v1");args=ap.parse_args()
    root=Path(args.root)
    targets={"BTC":(285.64,11680,-217.27),"ETH":(259.76,8960,-289.58),"SOL":(787.71,27310,-1069.61)}
    variants=[
      ("HFhit_LAST_HFthr","last","hf",False),
      ("HFhit_MEAN_HFthr","mean","hf",False),
      ("HFhit_MEAN_30thr","mean","bar",False),
      ("HFhit_LAST_30thr","last","bar",False),
      ("HFhit_LAST_HFthr_GAPCLEAR","last","hf",True),
      ("HFhit_MEAN_30thr_GAPCLEAR","mean","bar",True),
    ]
    combined={v[0]:{} for v in variants}
    for asset in ASSETS:
        # build twice only if gap behavior differs
        cache={}
        for clear in (False,True):
            ts,p,cols,meta=build_regular_1s(root,asset,clear_on_gap=clear)
            diag=hf_diagnostics(ts,p,cols,HORIZONS[asset])
            cache[clear]=(ts,p,cols,meta,diag)
            print(json.dumps({"kind":"HF_DIAG","asset":asset,"clear_gap":clear,
                              "selected":[d["shell_idx"] for d in diag if d["selected"]],
                              "hit_rates":{str(d["shell_idx"]):d["hit_rate"] for d in diag if d["selected"]},
                              "meta":meta}),flush=True)
        for name,bar_mode,thr_src,clear in variants:
            ts,p,cols,meta,diag=cache[clear]
            bars=build_30s_bars(ts,p,cols,bar_mode)
            sig,sel=signals_from_hf_selected(bars,diag,thr_src)
            summ,curve,events=run_strategy(asset,sig,None)
            combined[name][asset]=curve
            target=targets[asset]
            score=(abs(summ["final_pnl"]-target[0])/max(50,abs(target[0]))+
                   abs(summ["max_open_usd"]-target[1])/target[1]+
                   abs(summ["max_drawdown"]-target[2])/max(50,abs(target[2])))
            print(json.dumps({"kind":"VARIANT","variant":name,"asset":asset,"selected":sel,
                              "pnl":summ["final_pnl"],"max_open":summ["max_open_usd"],
                              "max_dd":summ["max_drawdown"],"final_open":summ["final_open_usd"],
                              "entries":summ["child_entries"],"exits":summ["child_exits"],
                              "nonzero":summ["nonzero_signal_bars"],"score":score,
                              "target":{"pnl":target[0],"max_open":target[1],"dd":target[2]}}),flush=True)
    for name in combined:
        curve,stats=combine_curves(combined[name])
        print(json.dumps({"kind":"COMBINED","variant":name,**stats}),flush=True)
    print("CALIBRATION_DONE",flush=True)

if __name__=="__main__": main()
