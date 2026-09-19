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

def build_true60(root, asset, progress=None):
    files = discover_files(root, asset)
    if not files:
        raise RuntimeError(f"no Binance files for {asset}")

    depth = [None]*14
    ready = False
    current_run = None

    minute_t = None
    sums = [0.0]*14
    counts = [0]*14
    sample_count = 0
    price_last = None
    bars = []

    gap_count = 0
    reset_count = 0
    record_count = 0
    near_samples = 0
    trusted_rejects = [0]*14
    used_samples = [0]*14

    pending_ts = None
    pending = []

    def clear_state():
        nonlocal depth
        depth = [None]*14

    def flush_group(ts, events):
        nonlocal ready, gap_count, reset_count, minute_t, sums, counts, sample_count, price_last, near_samples
        if ts is None:
            return
        saw_valid_near = False
        group_price = None
        for rec in events:
            kind = rec[0]
            if kind == "gap":
                gap_count += 1
                ready = False
                clear_state()
                continue
            if kind == "reset":
                reset_count += 1
                ready = True
                clear_state()
                continue
            if kind != "depth":
                continue
            _, actual, typ, mid, trusted_bid, trusted_ask, observed_bid, observed_ask, quality, bids, asks = rec
            lo,hi = TYPE_INFO[typ]
            if quality != VALID_QUALITY or not ready:
                for i in range(lo,hi):
                    depth[i] = None
                continue
            for j,i in enumerate(range(lo,hi)):
                upper = BOUNDS[i+1]
                b = float(bids[j])
                a = float(asks[j])
                cov_ok = True  # RELAXED sensitivity: use stored shell even beyond trusted snapshot coverage
                if cov_ok and math.isfinite(b) and math.isfinite(a) and (b+a) > 0:
                    depth[i] = (b,a)
                else:
                    depth[i] = None
                    trusted_rejects[i] += 1
            if typ == 1 and math.isfinite(mid) and mid > 0:
                saw_valid_near = True
                group_price = float(mid)

        if not ready or not saw_valid_near or group_price is None:
            return

        near_samples += 1
        mt = (ts // 60000) * 60000
        if minute_t is None:
            minute_t = mt
        elif mt != minute_t:
            b = finalize_minute(minute_t, price_last, sums, counts, sample_count)
            if b is not None:
                bars.append(b)
            minute_t = mt
            sums = [0.0]*14
            counts = [0]*14
            sample_count = 0
            price_last = None

        sample_count += 1
        price_last = group_price
        for i,ba in enumerate(depth):
            if ba is None:
                continue
            b,a = ba
            den = b+a
            if den <= 0:
                continue
            x = (b-a)/den
            if math.isfinite(x):
                sums[i] += x
                counts[i] += 1
                used_samples[i] += 1

    total_files = len(files)
    last_block = None
    for idx,(block_ts, run_name, path) in enumerate(files,1):
        if current_run != run_name:
            if pending:
                flush_group(pending_ts, pending)
                pending_ts, pending = None, []
            current_run = run_name
            ready = False
            clear_state()
        last_block = block_ts
        for rec in iter_block_records(path):
            record_count += 1
            ts = rec[1]
            if pending_ts is None:
                pending_ts = ts
                pending = [rec]
            elif ts == pending_ts:
                pending.append(rec)
            else:
                flush_group(pending_ts, pending)
                pending_ts = ts
                pending = [rec]
        if progress and (idx == 1 or idx % 100 == 0 or idx == total_files):
            progress(asset, idx, total_files, block_ts)

    if pending:
        flush_group(pending_ts, pending)
    b = finalize_minute(minute_t, price_last, sums, counts, sample_count)
    if b is not None:
        bars.append(b)

    bars.sort(key=lambda x:x["t"])
    dedup = []
    for b in bars:
        if dedup and dedup[-1]["t"] == b["t"]:
            # Should be rare; merge by keeping later price and weighted shell means.
            prev = dedup[-1]
            total_samples = prev["samples"] + b["samples"]
            new_imb = []
            new_counts = []
            for i in range(14):
                c1,c2 = prev["counts"][i],b["counts"][i]
                x1,x2 = prev["imb"][i],b["imb"][i]
                c = c1+c2
                new_counts.append(c)
                if c:
                    s = (0.0 if x1 is None else x1*c1) + (0.0 if x2 is None else x2*c2)
                    new_imb.append(s/c)
                else:
                    new_imb.append(None)
            prev["price"] = b["price"]
            prev["imb"] = new_imb
            prev["counts"] = new_counts
            prev["samples"] = total_samples
        else:
            dedup.append(b)
    bars = dedup

    meta = {
        "asset":asset,
        "files":total_files,
        "record_count":record_count,
        "gap_count":gap_count,
        "reset_count":reset_count,
        "near_samples":near_samples,
        "minute_bars":len(bars),
        "start_ms":bars[0]["t"] if bars else None,
        "end_ms":bars[-1]["t"] if bars else None,
        "trusted_rejects":trusted_rejects,
        "used_samples":used_samples,
    }
    return bars, meta

def depth_diagnostics(asset, bars, horizon_s):
    by_t = {b["t"]:b for b in bars}
    out = []
    hms = horizon_s*1000
    for i in range(14):
        vals = [b["imb"][i] for b in bars if b["imb"][i] is not None and math.isfinite(b["imb"][i])]
        pos = [x for x in vals if x > 0]
        neg = [x for x in vals if x < 0]
        pos_thr = quantile_linear(pos,0.90)
        neg_thr = quantile_linear(neg,0.10)
        hits = 0
        n = 0
        xs,ys = [],[]
        for b in bars:
            x = b["imb"][i]
            if x is None or not math.isfinite(x):
                continue
            fb = by_t.get(b["t"]+hms)
            if fb is None:
                continue
            p0,p1 = b["price"],fb["price"]
            if not (p0 and p1 and p0>0 and p1>0):
                continue
            r = p1/p0 - 1.0
            n += 1
            if sign(x) == sign(r):
                hits += 1
            xs.append(x); ys.append(r)
        hit_rate = hits/n if n else None
        corr = pearson(xs,ys)
        valid_counts = [b["counts"][i] for b in bars if b["counts"][i] > 0]
        out.append({
            "asset":asset,
            "shell_idx":i,
            "shell":RANGES[i],
            "upper_bps":BOUNDS[i+1],
            "minute_valid_count":len(vals),
            "mean_samples_per_valid_minute": (sum(valid_counts)/len(valid_counts)) if valid_counts else None,
            "pos_threshold":pos_thr,
            "neg_threshold":neg_thr,
            "N":n,
            "hit_rate":hit_rate,
            "corr":corr,
            "selected":bool(hit_rate is not None and hit_rate > 0.50),
        })
    return out

def build_signals(bars, diagnostics):
    selected = [d for d in diagnostics if d["selected"] and d["pos_threshold"] is not None and d["neg_threshold"] is not None]
    signals = []
    for b in bars:
        bull=bear=0
        for d in selected:
            x=b["imb"][d["shell_idx"]]
            if x is None:
                continue
            if x >= d["pos_threshold"]:
                bull += 1
            elif x <= d["neg_threshold"]:
                bear += 1
        signals.append({"t":b["t"],"price":b["price"],"bull":bull,"bear":bear,"net":bull-bear})
    return signals, [d["shell_idx"] for d in selected]

def run_strategy(asset, signals, cap_usd):
    cap_children = None if cap_usd is None else int(round(cap_usd/CHILD_USD))
    children = deque()
    direction = 0
    dir_start = None
    sum_inv = 0.0
    realized = 0.0
    fee_per_child = CHILD_USD * FEE_RATE
    blocked = 0
    entries = exits = 0
    long_entries = short_entries = 0
    nonzero_signal_minutes = 0
    ignored_minhold = 0
    max_open = max_long = max_short = 0.0
    exposure_sum = 0.0
    mins_long=mins_short=mins_flat=0
    curve=[]
    events=[]
    peak=0.0
    max_dd=0.0

    def add(n, dirn, t, price, reason):
        nonlocal realized,sum_inv,entries,long_entries,short_entries,blocked,direction,dir_start
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
            if dirn>0: long_entries += actual
            else: short_entries += actual
            events.append((t,reason,dirn,actual,price,len(children),realized,blocked_now))
        elif blocked_now:
            events.append((t,"BLOCKED",dirn,0,price,len(children),realized,blocked_now))
        return actual

    def close(n,t,price,reason):
        nonlocal realized,sum_inv,exits,direction,dir_start
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
        events.append((t,reason,dirn,-actual,price,len(children),realized,0))
        if not children:
            direction=0
            dir_start=None
            sum_inv=0.0
        return actual

    for s in signals:
        t,p,net=s["t"],s["price"],s["net"]
        if net:
            nonzero_signal_minutes += 1
        if direction == 0:
            if net>0: add(abs(net),1,t,p,"OPEN_LONG")
            elif net<0: add(abs(net),-1,t,p,"OPEN_SHORT")
        elif net == 0:
            pass
        elif sign(net) == direction:
            add(abs(net),direction,t,p,"ADD")
        else:
            if dir_start is not None and (t-dir_start) < MIN_HOLD_S*1000:
                ignored_minhold += 1
            else:
                req=abs(net)
                c=close(req,t,p,"PARTIAL_CLOSE")
                rem=req-c
                if rem>0 and direction==0:
                    nd=sign(net)
                    add(rem,nd,t,p,"REVERSE_OPEN")

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
        if direction>0:mins_long+=1
        elif direction<0:mins_short+=1
        else:mins_flat+=1
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
        "nonzero_signal_minutes":nonzero_signal_minutes,
        "ignored_reverse_minhold_minutes":ignored_minhold,
        "max_long_usd":max_long,
        "max_short_usd":max_short,
        "avg_abs_open_usd":exposure_sum/len(curve) if curve else 0.0,
        "minutes_long":mins_long,
        "minutes_short":mins_short,
        "minutes_flat":mins_flat,
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
        "asset","shell_idx","shell","upper_bps","minute_valid_count","mean_samples_per_valid_minute",
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

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--root",default="/data/spot-depth-production-v1")
    ap.add_argument("--out",default="/data/true60_results")
    args=ap.parse_args()
    root=Path(args.root)
    out=Path(args.out)
    out.mkdir(parents=True,exist_ok=True)
    started=time.time()

    all_diag=[]
    summaries=[]
    asset_meta={}
    signals_by_asset={}
    curves_by_mode={"cap30000":{},"uncapped":{}}

    run_manifest={
        "name":"true_60s_signal_strategy_relaxed_coverage_sensitivity",
        "created_utc":datetime.now(timezone.utc).isoformat(),
        "dataset_root":str(root),
        "venue":"Binance Spot",
        "assets":ASSETS,
        "horizons_s":HORIZONS,
        "child_usd":CHILD_USD,
        "fee_bps_per_side":FEE_BPS,
        "min_hold_s":MIN_HOLD_S,
        "cap_usd_per_asset":CAP_USD,
        "discovery_warning":"Discovery / in-sample only. Thresholds, shell selection, and horizons were chosen using this dataset. Not an out-of-sample or live-profit validation.",
        "minute_method":"Reconstruct latest 14-shell state at each VALID near-depth sample (~1s); apply same-timestamp near/mid/far updates together; compute shell imbalance from base-asset bid/ask qty; average imbalance within each UTC 60s bin; price is last VALID near mid in bin.",
        "coverage_rule":"RELAXED SENSITIVITY ONLY: use stored shell depth regardless of trusted coverage; Non-VALID quality excluded; GAP clears state until RESET. Not the primary governed result.",
        "threshold_rule":"For each shell on 60s imbalance: positive threshold = 90th percentile of positive values; negative threshold = 10th percentile of negative values; linear interpolation.",
        "hit_rule":"sign(60s imbalance_t) == sign(Binance Spot local-mid return from bar t to exact t+horizon); selected iff hit_rate > 0.50.",
        "vote_rule":"Selected shell votes bull when imbalance >= positive threshold, bear when <= negative threshold. net=bull-bear.",
        "execution_rule":"One decision per 60s bar; same-direction net adds 10 USD children; opposite net after 300s minimum direction hold closes FIFO children by |net| and only opens reverse children if old direction is fully closed and votes remain.",
        "fees":"Taker 3.87 bps on each child entry and each child exit; no slippage, impact, funding, borrow, margin or liquidation model.",
        "force_close_at_end":False,
    }

    for asset in ASSETS:
        print(json.dumps({"kind":"asset_start","asset":asset}),flush=True)
        bars,meta=build_true60(root,asset,progress=progress)
        asset_meta[asset]=meta
        horizon=HORIZONS[asset]
        diag=depth_diagnostics(asset,bars,horizon)
        all_diag.extend(diag)
        signals,selected=build_signals(bars,diag)
        signals_by_asset[asset]=signals
        meta["horizon_s"]=horizon
        meta["selected_shells"]=selected
        meta["selected_shell_ranges"]=[RANGES[i] for i in selected]

        write_diag(out/f"{asset}_60s_depth_diagnostics.csv",diag)
        write_signals(out/f"{asset}_60s_vote_signal.csv",signals)
        svg_vote(out/f"{asset}_true60_vote_state.svg",f"{asset} true-60s Binance Spot vote state",signals)

        for cap in (CAP_USD,None):
            mode="cap30000" if cap is not None else "uncapped"
            summary,curve,events=run_strategy(asset,signals,cap)
            summary["horizon_s"]=horizon
            summary["selected_shells"]=";".join(str(i) for i in selected)
            summary["selected_shell_ranges"]=";".join(RANGES[i] for i in selected)
            summaries.append(summary)
            curves_by_mode[mode][asset]=curve
            write_curve(out/f"{asset}_{mode}_equity_1min.csv",curve)
            write_events(out/f"{asset}_{mode}_events.csv",events)
        svg_equity(out/f"{asset}_cap30k_vs_uncapped_true60_equity.svg",
                   f"{asset} true-60s equity: cap30k vs uncapped",
                   {"cap30k":curves_by_mode["cap30000"][asset],"uncapped":curves_by_mode["uncapped"][asset]})
        print(json.dumps({"kind":"asset_done","asset":asset,"bars":len(bars),"selected":selected}),flush=True)

    write_diag(out/"all_depth_diagnostics_true60.csv",all_diag)
    write_csv(out/"summary_true60.csv",summaries)

    combined_rows=[]
    combined_curves={}
    for mode in ("cap30000","uncapped"):
        curve,stats=combine_curves(curves_by_mode[mode])
        combined_curves[mode]=curve
        write_combined_curve(out/f"ALL_{mode}_equity_1min.csv",curve)
        row={"scope":"ALL","cap_mode":mode,**stats}
        row["sum_asset_final_pnl"]=sum(s["final_pnl"] for s in summaries if s["cap_mode"]==("cap30000" if mode=="cap30000" else "uncapped"))
        row["sum_asset_realized_pnl"]=sum(s["realized_pnl"] for s in summaries if s["cap_mode"]==("cap30000" if mode=="cap30000" else "uncapped"))
        row["sum_asset_unrealized_pnl"]=sum(s["unrealized_pnl"] for s in summaries if s["cap_mode"]==("cap30000" if mode=="cap30000" else "uncapped"))
        combined_rows.append(row)
    write_csv(out/"combined_summary_true60.csv",combined_rows)
    svg_equity(out/"ALL_cap30k_vs_uncapped_true60_equity.svg",
               "BTC+ETH+SOL true-60s combined equity",
               {"cap30k":[(p[0],p[1]) for p in combined_curves["cap30000"]],
                "uncapped":[(p[0],p[1]) for p in combined_curves["uncapped"]]})

    run_manifest["asset_meta"]=asset_meta
    run_manifest["elapsed_seconds"]=time.time()-started
    (out/"run_manifest_true60.json").write_text(json.dumps(run_manifest,indent=2,ensure_ascii=False),encoding="utf-8")

    done={
        "ok":True,
        "created_utc":datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds":run_manifest["elapsed_seconds"],
        "summary_file":"summary_true60.csv",
        "combined_summary_file":"combined_summary_true60.csv",
        "diagnostics_file":"all_depth_diagnostics_true60.csv",
        "asset_meta":asset_meta,
    }
    (out/"DONE.json").write_text(json.dumps(done,indent=2,ensure_ascii=False),encoding="utf-8")
    print(json.dumps({"kind":"DONE","out":str(out),"elapsed_seconds":done["elapsed_seconds"]}),flush=True)

if __name__=="__main__":
    main()
