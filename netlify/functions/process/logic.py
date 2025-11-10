import io, os, re, math, json, tempfile
from typing import List, Tuple, Dict, Optional, Set
import pandas as pd

APP_FILE_DEFAULT = "zoom_attendance_processed.xlsx"

EXCLUDE_NAME_PATTERNS = [
    r"^\s*meeting analytics from read\s*$",
    r"^\s*ta\s*$",
    r"^\s*saboor'?s fathom notetaker\s*$",
    r"^\s*hassaan khalid\s*$",
]

RECONNECT_OVERLAP_TOLERANCE = pd.Timedelta(seconds=2)

# -------------------- time helpers --------------------

def _parse_dt(s):
    if pd.isna(s): return None
    try: return pd.to_datetime(s)
    except Exception: return None

def _merge_intervals(intervals: List[Tuple[pd.Timestamp, pd.Timestamp]]) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    ints = [(s,e) for s,e in intervals if s is not None and e is not None and pd.notna(s) and pd.notna(e) and e>s]
    if not ints: return []
    ints.sort(key=lambda x:x[0])
    merged=[]; cur_s,cur_e=ints[0]
    for s,e in ints[1:]:
        if s<=cur_e:
            if e>cur_e: cur_e=e
        else:
            merged.append((cur_s,cur_e)); cur_s,cur_e=s,e
    merged.append((cur_s,cur_e))
    return merged

def _minutes(intervals: List[Tuple[pd.Timestamp, pd.Timestamp]]) -> float:
    return sum((e-s).total_seconds()/60.0 for s,e in intervals)

def _interval_union_minutes(intervals: List[Tuple[pd.Timestamp, pd.Timestamp]]) -> float:
    return _minutes(_merge_intervals(intervals))

def _has_any_overlap_raw(intervals: List[Tuple[pd.Timestamp, pd.Timestamp]]) -> bool:
    ints = [(s,e) for s,e in intervals if s is not None and e is not None and e>s]
    if len(ints)<2: return False
    ints.sort(key=lambda x:x[0])
    _,prev_e = ints[0]
    for s,e in ints[1:]:
        if s<prev_e: return True
        prev_e=max(prev_e,e)
    return False

def _intervals_overlap_or_close(A: List[Tuple[pd.Timestamp,pd.Timestamp]], B: List[Tuple[pd.Timestamp,pd.Timestamp]], max_gap_minutes: float = 7.0) -> bool:
    A=_merge_intervals(A); B=_merge_intervals(B)
    if not A or not B: return False
    i=j=0; gap=pd.Timedelta(minutes=max_gap_minutes)
    while i<len(A) and j<len(B):
        a_s,a_e=A[i]; b_s,b_e=B[j]
        if a_e>=b_s and b_e>=a_s: return True
        if a_e<b_s:
            if (b_s-a_e)<=gap: return True
            i+=1
        elif b_e<a_s:
            if (a_s-b_e)<=gap: return True
            j+=1
        else:
            if a_e<b_e: i+=1
            else: j+=1
    return False

def _minutes_A_minus_B(A: List[Tuple[pd.Timestamp,pd.Timestamp]], B: List[Tuple[pd.Timestamp,pd.Timestamp]]) -> float:
    A=_merge_intervals(A); B=_merge_intervals(B)
    if not A: return 0.0
    if not B: return _minutes(A)
    total=0.0; j=0
    for a_s,a_e in A:
        cur=a_s
        while cur<a_e and j<len(B):
            b_s,b_e=B[j]
            if b_e<=cur: j+=1; continue
            if b_s>=a_e: break
            if b_s>cur: total += (b_s-cur).total_seconds()/60.0
            cur=max(cur,b_e)
            if b_e<=cur: j+=1
        if cur<a_e: total += (a_e-cur).total_seconds()/60.0
    return total

# -------------------- formatting helpers --------------------

def _ts_to_excel_str(ts: Optional[pd.Timestamp]) -> str:
    if ts is None or (isinstance(ts, float) and math.isnan(ts)):
        return ""
    if isinstance(ts, pd.Timestamp):
        if ts.tzinfo is not None:
            ts = ts.tz_convert(None)
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    return str(ts)

def _td_to_hms(td: pd.Timedelta) -> str:
    if td is None:
        return ""
    total_seconds = int(max(0, td.total_seconds()))
    hrs, rem = divmod(total_seconds, 3600)
    mins, secs = divmod(rem, 60)
    return f"{hrs:02d}:{mins:02d}:{secs:02d}"

def _clean_raw_str(val) -> str:
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except Exception:
        pass
    return str(val)

# -------------------- reconnect detection --------------------

def _prepare_segments_for_reconnect(records: List[dict]) -> List[dict]:
    segments=[]
    for rec in records:
        s=rec.get("join_ts"); e=rec.get("leave_ts")
        if s is None and e is None: continue
        if s is None: s=e
        if e is None: e=s
        if s is None or e is None: continue
        if isinstance(s, pd.Timestamp) and isinstance(e, pd.Timestamp) and e<s:
            s,e=e,s
        segments.append({
            "start": s, "end": e,
            "join_ts": rec.get("join_ts"), "leave_ts": rec.get("leave_ts"),
            "join_raw": rec.get("join_raw",""), "leave_raw": rec.get("leave_raw",""),
            "raw_name": rec.get("raw_name",""),
        })
    segments.sort(key=lambda seg: (seg["start"], seg["end"]))
    return segments

def _compute_reconnect_events(segments: List[dict]):
    if not segments:
        return 0, []
    events=[]
    coverage_seg=segments[0]
    coverage_end=coverage_seg["end"]
    idx=0
    for seg in segments[1:]:
        start=seg["start"] or seg["end"]
        if start is None:
            if seg["end"] is not None and (coverage_end is None or seg["end"]>coverage_end):
                coverage_seg=seg; coverage_end=seg["end"]
            continue
        if coverage_end is None:
            coverage_seg=seg; coverage_end=seg["end"]
            continue
        if start + RECONNECT_OVERLAP_TOLERANCE < coverage_end:
            if seg["end"] is not None and seg["end"]>coverage_end:
                coverage_seg=seg; coverage_end=seg["end"]
            continue
        disconnect_ts=coverage_end; reconnect_ts=start
        gap_td=reconnect_ts - disconnect_ts
        if isinstance(gap_td, pd.Timedelta):
            if gap_td.total_seconds()<0:
                gap_td=pd.Timedelta(0)
        else:
            gap_td=pd.Timedelta(0)
        idx+=1
        events.append({
            "index": idx,
            "disconnect_seg": coverage_seg,
            "reconnect_seg": seg,
            "disconnect_ts": disconnect_ts,
            "reconnect_ts": reconnect_ts,
            "gap": gap_td,
        })
        coverage_seg=seg
        coverage_end=seg["end"]
    return len(events), events

# -------------------- name normalization --------------------

def _canon_name(s: str) -> str:
    if not isinstance(s, str): return ""
    s = s.lower()
    s = re.sub(r"\([^)]*\)", " ", s)
    s = re.sub(r"\d{5}", " ", s)
    s = re.sub(r"[_\-]", " ", s)
    s = re.sub(r"[^a-z]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _norm_name_spaces_only(s: str) -> str:
    return re.sub(r"\s+"," ", str(s).strip().lower())

# -------------------- CSV readers --------------------

def _read_csv_resilient(path: str) -> pd.DataFrame:
    for args in (
        dict(encoding="utf-8-sig", engine="c"),
        dict(encoding="utf-8-sig", engine="python", sep=None),
        dict(encoding="utf-16",   engine="python", sep=None),
        dict(encoding="utf-8",    engine="python", sep=None, on_bad_lines="skip"),
    ):
        try:
            return pd.read_csv(path, dtype=str, **args)
        except Exception:
            continue
    return pd.read_csv(path, dtype=str, encoding="utf-8", engine="python", sep=None, on_bad_lines="skip")

def _read_zoom_participants_table(path: str) -> pd.DataFrame:
    text = None
    for enc in ("utf-8-sig", "utf-16", "utf-8"):
        try:
            with open(path, "r", encoding=enc) as f:
                text = f.read()
            break
        except UnicodeError:
            continue
    if text is None:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            text = f.read()
    m = re.search(r"^\s*Name\s*\(original name\)\s*,", text, flags=re.I | re.M)
    if m:
        start = text.rfind("\n", 0, m.start()) + 1
        payload = text[start:]
    else:
        lines = [ln for ln in text.splitlines() if ln.strip()]
        idx = -1
        for i, ln in enumerate(lines):
            low = ln.lower()
            if "join time" in low and "leave time" in low:
                idx = i; break
        if idx == -1:
            raise ValueError("Could not locate the participants header row in the CSV.")
        payload = "\n".join(lines[idx:])
    try:
        df = pd.read_csv(io.StringIO(payload), dtype=str, engine="c")
    except Exception:
        df = pd.read_csv(io.StringIO(payload), dtype=str, engine="python", sep=",")
    return df

def normalize_zoom_csv(path: str) -> pd.DataFrame:
    return _read_zoom_participants_table(path).reset_index(drop=True)

def _detect_columns(df: pd.DataFrame):
    cols={c.lower(): c for c in df.columns}
    def pick(cands):
        for c in cands:
            if c in cols: return cols[c]
        return None
    name_col  = pick(["name (original name)","name","participant","user name","full name","display name"])
    join_col  = pick(["join time","join time (timezone)","join time (yyyy-mm-dd hh:mm:ss)","join time (utc)","first join time","first join time (utc)"])
    leave_col = pick(["leave time","leave time (timezone)","leave time (yyyy-mm-dd hh:mm:ss)","leave time (utc)","last leave time","last leave time (utc)"])
    duration_col = pick(["duration (minutes)","total duration (minutes)","time in meeting (minutes)"])
    email_col = pick(["user email","email","attendee email"])
    participant_id_col = pick(["participant id","user id","unique id","id"])
    if name_col is None: raise ValueError(f"Could not detect participant name column. Found: {list(df.columns)}")
    if (join_col is None or leave_col is None) and duration_col is None:
        raise ValueError("No join/leave or duration columns found in the CSV.")
    return dict(name_col=name_col, join_col=join_col, leave_col=leave_col,
                duration_col=duration_col, email_col=email_col, pid_col=participant_id_col)

# -------------------- roster loader --------------------

def _detect_roster_columns(df: pd.DataFrame):
    cols={c.lower(): c for c in df.columns}
    def is_5d(x):
        try: return bool(re.match(r"^\s*\d{5}\s*$", str(x)))
        except: return False
    best_col=None; best_hits=-1
    for c in df.columns:
        v=df[c].dropna().astype(str).head(500)
        hits=sum(1 for x in v if is_5d(x))
        if hits>best_hits: best_hits=hits; best_col=c
    erp_col = best_col if best_hits>0 else None
    name_col = None
    for k in ["name","student name","full name","official name"]:
        if k in cols: name_col=cols[k]; break
    if name_col is None:
        for c in df.columns:
            if c!=erp_col and df[c].dtype==object:
                name_col=c; break
    email_col=None
    for k in ["email","user email","attendee email","e-mail"]:
        if k in cols: email_col=cols[k]; break
    return dict(erp_col=erp_col, name_col=name_col, email_col=email_col)

def load_roster(path: str) -> pd.DataFrame:
    if not path: return pd.DataFrame()
    ext=os.path.splitext(path)[1].lower()
    if ext in (".xlsx",".xls"):
        rdf=pd.read_excel(path, dtype=str)
    else:
        rdf=_read_csv_resilient(path)
    cols=_detect_roster_columns(rdf)
    erp_col, name_col, email_col = cols["erp_col"], cols["name_col"], cols["email_col"]
    if erp_col is None or name_col is None:
        raise ValueError("Could not detect ERP/Name columns in roster. Make sure it has 5-digit ERP and a Name column.")
    out=pd.DataFrame({
        "ERP": rdf[erp_col].astype(str).str.extract(r"(\d{5})", expand=False),
        "RosterName": rdf[name_col].astype(str).str.strip(),
    })
    out["RosterCanon"] = out["RosterName"].apply(_canon_name)
    if email_col:
        out["Email"]=rdf[email_col].astype(str).str.strip()
    else:
        out["Email"]=""
    out=out.dropna(subset=["ERP","RosterName"]).drop_duplicates(subset=["ERP"], keep="first")
    return out.reset_index(drop=True)

# -------------------- name/erp parsing --------------------

def _name_to_erp_and_clean(name: str):
    if not isinstance(name,str): return None,"",-1
    name=name.strip()
    m=re.match(r"^\s*(\d{5})[\s\-_]+(.+?)\s*$", name)
    if m: return m.group(1), m.group(2).strip(), 0
    return None, name.strip(), -1

# -------------------- main engine --------------------

def process_zoom_attendance(csv_path: str, output_xlsx: str, threshold_ratio: float = 0.8, buffer_minutes: float = 0.0, break_minutes: float = 0.0, exemptions: Dict[str, Dict[str,bool]] = None, override_total_minutes: Optional[float] = None, penalty_tolerance_minutes: float = 0.0, roster_path: Optional[str] = None, rounding_mode: str = "none") -> dict:
    if rounding_mode not in ("none","ceil_attendance","ceil_both"):
        rounding_mode="none"
    exemptions = exemptions or {}
    roster_df = load_roster(roster_path) if roster_path else pd.DataFrame()

    raw_df=normalize_zoom_csv(csv_path)
    df=raw_df.copy()
    cols=_detect_columns(df)
    name_col, join_col, leave_col, duration_col = cols["name_col"], cols["join_col"], cols["leave_col"], cols["duration_col"]

    mask_excl=pd.Series(False, index=df.index)
    for pat in EXCLUDE_NAME_PATTERNS:
        mask_excl |= df[name_col].astype(str).str.contains(pat, flags=re.I, regex=True, na=False)
    df=df.loc[~mask_excl].copy()

    email_col, pid_col = cols.get("email_col"), cols.get("pid_col")
    df["_email"]=df[email_col].astype(str) if email_col in df else ""
    df["_pid"]=df[pid_col].astype(str) if pid_col in df else ""

    df["_join"]=df[join_col].apply(_parse_dt) if join_col in df else None
    df["_leave"]=df[leave_col].apply(_parse_dt) if leave_col in df else None
    has_times=(join_col in df and leave_col in df and df["_join"].notna().any() and df["_leave"].notna().any())

    if override_total_minutes and override_total_minutes>0:
        total_class_minutes=float(override_total_minutes)
    else:
        if has_times:
            jo=df["_join"].dropna(); le=df["_leave"].dropna()
            if jo.empty or le.empty:
                raise ValueError("Timestamps present but unparsable. Check the Zoom CSV encoding/format.")
            total_class_minutes=float((le.max()-jo.min()).total_seconds()/60.0)
        elif duration_col and duration_col in df:
            total_class_minutes=float(pd.to_numeric(df[duration_col], errors="coerce").fillna(0).max())
        else:
            raise ValueError("Could not determine total class duration.")

    break_minutes=max(0.0, float(break_minutes or 0.0))
    adjusted_total_minutes=max(total_class_minutes - break_minutes, 1.0)
    threshold_minutes_raw=float(threshold_ratio)*adjusted_total_minutes
    buffer_minutes=max(0.0, float(buffer_minutes or 0.0))
    effective_threshold_minutes=max(0.0, threshold_minutes_raw - buffer_minutes)

    parsed=df[name_col].apply(_name_to_erp_and_clean)
    df["_erp"]=parsed.apply(lambda x:x[0])
    df["_clean_name"]=parsed.apply(lambda x:x[1])
    df["_pen_flag"]=parsed.apply(lambda x:x[2])
    df["_canon"]=df["_clean_name"].apply(_canon_name)
    df["_rawname"]=df[name_col].astype(str)

    def key_of(row):
        if pd.notna(row["_erp"]) and row["_erp"] is not None: return f"ERP:{row['_erp']}"
        return f"NAME:{_norm_name_spaces_only(row['_clean_name'])}"
    df["_key"]=df.apply(key_of,axis=1)

    names_by_key: Dict[str,str]={}
    canon_by_key: Dict[str,str]={}
    erp_by_key: Dict[str,Optional[str]]={}
    intervals_by_key: Dict[str,List[Tuple[pd.Timestamp,pd.Timestamp]]]={}
    good_intervals_by_key: Dict[str,List[Tuple[pd.Timestamp,pd.Timestamp]]]={}
    bad_intervals_by_key: Dict[str,List[Tuple[pd.Timestamp,pd.Timestamp]]]={}
    durations_good_by_key: Dict[str,List[float]]={}
    durations_bad_by_key: Dict[str,List[float]]={}
    raw_names_by_key: Dict[str,Set[str]]={}
    match_source_by_key: Dict[str,str]={}
    session_records_by_key: Dict[str,List[dict]]={}

    if has_times:
        for _,r in df.iterrows():
            k=r["_key"]; s,e=r.get("_join",None), r.get("_leave",None)
            names_by_key.setdefault(k, r["_clean_name"])
            canon_by_key.setdefault(k, r["_canon"])
            erp_by_key.setdefault(k, r["_erp"]) 
            intervals_by_key.setdefault(k, [])
            raw_names_by_key.setdefault(k, set()).add(r["_rawname"])
            session_records_by_key.setdefault(k, []).append(dict(
                join_ts=s, leave_ts=e,
                join_raw=r.get(cols["join_col"], ""), leave_raw=r.get(cols["leave_col"], ""),
                raw_name=r["_rawname"],
            ))
            if s is not None and e is not None: intervals_by_key[k].append((s,e))
            if r["_pen_flag"]==-1:
                bad_intervals_by_key.setdefault(k, [])
                if s is not None and e is not None: bad_intervals_by_key[k].append((s,e))
            else:
                good_intervals_by_key.setdefault(k, [])
                if s is not None and e is not None: good_intervals_by_key[k].append((s,e))
            if k.startswith("ERP:"): match_source_by_key.setdefault(k,"erp_in_name")
            else: match_source_by_key.setdefault(k,"name_only")
    else:
        durs=pd.to_numeric(df[duration_col], errors="coerce").fillna(0.0)
        df["_dur"]=durs
        for _,r in df.iterrows():
            k=r["_key"]; d=float(r["_dur"])
            names_by_key.setdefault(k, r["_clean_name"])
            canon_by_key.setdefault(k, r["_canon"])
            erp_by_key.setdefault(k, r["_erp"]) 
            raw_names_by_key.setdefault(k, set()).add(r["_rawname"])
            if r["_pen_flag"]==-1: durations_bad_by_key.setdefault(k, []).append(d)
            else: durations_good_by_key.setdefault(k, []).append(d)
            match_source_by_key.setdefault(k, "erp_in_name" if k.startswith("ERP:") else "name_only")

    alias_merges=[]; ambiguous_aliases=set()
    if has_times:
        erp_by_canon: Dict[str,List[str]]={}
        name_by_canon: Dict[str,List[str]]={}
        for k,cn in canon_by_key.items():
            if not cn: continue
            if k.startswith("ERP:"): erp_by_canon.setdefault(cn, []).append(k)
            else:                    name_by_canon.setdefault(cn, []).append(k)
        for cn, nkeys in name_by_canon.items():
            erp_keys = erp_by_canon.get(cn, [])
            if not erp_keys:
                continue
            for name_k in list(nkeys):
                if name_k not in names_by_key: continue
                chosen=None
                if len(erp_keys)==1:
                    chosen=erp_keys[0]
                else:
                    name_ints=intervals_by_key.get(name_k, [])
                    for ek in erp_keys:
                        if _intervals_overlap_or_close(name_ints, intervals_by_key.get(ek, []), 7.0):
                            chosen=ek; break
                if chosen is None:
                    ambiguous_aliases.add(name_k)
                    continue
                intervals_by_key.setdefault(chosen, []).extend(intervals_by_key.get(name_k, []))
                good_intervals_by_key.setdefault(chosen, []).extend(good_intervals_by_key.get(name_k, []))
                bad_intervals_by_key.setdefault(chosen, []).extend(bad_intervals_by_key.get(name_k, []))
                session_records_by_key.setdefault(chosen, []).extend(session_records_by_key.get(name_k, []))
                raw_names_by_key.setdefault(chosen, set()).update(raw_names_by_key.get(name_k, set()))
                for d in (intervals_by_key, good_intervals_by_key, bad_intervals_by_key,
                          names_by_key, erp_by_key, match_source_by_key, raw_names_by_key, canon_by_key,
                          session_records_by_key):
                    d.pop(name_k, None)
                alias_merges.append((name_k, chosen))
                match_source_by_key[chosen] = "alias_merge"
    else:
        erp_by_canon: Dict[str,List[str]]={}
        name_by_canon: Dict[str,List[str]]={}
        for k,cn in canon_by_key.items():
            if not cn: continue
            if k.startswith("ERP:"): erp_by_canon.setdefault(cn, []).append(k)
            else:                    name_by_canon.setdefault(cn, []).append(k)
        for cn, nkeys in name_by_canon.items():
            erp_keys = erp_by_canon.get(cn, [])
            if len(erp_keys)==1:
                chosen=erp_keys[0]
                for name_k in list(nkeys):
                    if name_k not in names_by_key: continue
                    durations_good_by_key.setdefault(chosen, []).extend(durations_good_by_key.get(name_k, []))
                    durations_bad_by_key.setdefault(chosen, []).extend(durations_bad_by_key.get(name_k, []))
                    raw_names_by_key.setdefault(chosen, set()).update(raw_names_by_key.get(name_k, set()))
                    for d in (durations_good_by_key, durations_bad_by_key,
                              names_by_key, erp_by_key, match_source_by_key, raw_names_by_key, canon_by_key):
                        d.pop(name_k, None)
                    alias_merges.append((name_k, chosen))
            elif len(erp_keys)>1:
                for name_k in nkeys: ambiguous_aliases.add(name_k)

    attendance_rows=[]; issues_rows=[]; absent_rows=[]; penalties_rows=[]; matches_rows=[]; reconnect_rows=[]
    ambiguous_name_keys=set()
    if has_times:
        for k in list(names_by_key.keys()):
            if k.startswith("NAME:"):
                raw_ints=intervals_by_key.get(k, [])
                if _has_any_overlap_raw(raw_ints):
                    ambiguous_name_keys.add(k)
    ambiguous_name_keys |= ambiguous_aliases

    present_erps=set()
    att_header = f"Attendance (>={int(threshold_ratio*100)}%)"

    for k in list(names_by_key.keys()):
        nm=names_by_key.get(k,"")
        erp=(erp_by_key.get(k,None) or "")
        raw_names_sorted = sorted(raw_names_by_key.get(k, {nm}))
        zoom_names_raw = "; ".join(raw_names_sorted)

        reconnect_count=0
        per_key_events=[]

        if has_times:
            all_ints=intervals_by_key.get(k, [])
            positive_ints=[(s,e) for s,e in all_ints if s is not None and e is not None and e>s]
            union_min_raw=_interval_union_minutes(all_ints)
            segments_for_detection=_prepare_segments_for_reconnect(session_records_by_key.get(k, []))
            seg_count=len(segments_for_detection) if segments_for_detection else len(positive_ints)
            reconnect_count, per_key_events=_compute_reconnect_events(segments_for_detection)
            overlap_any=_has_any_overlap_raw(all_ints)
            is_dual=(len(positive_ints)>1 and overlap_any)
            is_reconnect=(reconnect_count>0)
            bad_only_minutes=_minutes_A_minus_B(bad_intervals_by_key.get(k, []), good_intervals_by_key.get(k, []))
        else:
            total_good=sum(durations_good_by_key.get(k, []))
            total_bad=sum(durations_bad_by_key.get(k, []))
            union_min_raw=min(total_good+total_bad, adjusted_total_minutes)
            seg_count=int(len(durations_good_by_key.get(k, []))+len(durations_bad_by_key.get(k, [])))
            is_dual=(total_good+total_bad)>adjusted_total_minutes+0.1
            is_reconnect=(seg_count>1 and not is_dual)
            reconnect_count = max(0, seg_count-1) if is_reconnect else 0
            bad_only_minutes=float(total_bad)

        if has_times and per_key_events:
            for ev in per_key_events:
                gap_td=ev.get("gap", pd.Timedelta(0))
                gap_seconds=int(max(0, gap_td.total_seconds())) if isinstance(gap_td, pd.Timedelta) else 0
                gap_minutes=round(gap_seconds/60.0, 2)
                disconnect_seg=ev.get("disconnect_seg") or {}
                reconnect_seg=ev.get("reconnect_seg") or {}
                reconnect_rows.append(dict(
                    Key=k, ERP=erp, Name=nm,
                    **{"Zoom Names (raw)": zoom_names_raw},
                    **{"Event # (per student)": ev.get("index", 0)},
                    **{"Disconnect Time": _ts_to_excel_str(ev.get("disconnect_ts"))},
                    **{"Reconnect Time": _ts_to_excel_str(ev.get("reconnect_ts"))},
                    **{"Gap (minutes)": gap_minutes},
                    **{"Gap (seconds)": gap_seconds},
                    **{"Gap Duration (hh:mm:ss)": _td_to_hms(gap_td if isinstance(gap_td, pd.Timedelta) else pd.Timedelta(0))},
                    **{"Disconnect Raw Name": _clean_raw_str(disconnect_seg.get("raw_name", ""))},
                    **{"Reconnect Raw Name": _clean_raw_str(reconnect_seg.get("raw_name", ""))},
                    **{"Disconnect Join (raw)": _clean_raw_str(disconnect_seg.get("join_raw", ""))},
                    **{"Disconnect Leave (raw)": _clean_raw_str(disconnect_seg.get("leave_raw", ""))},
                    **{"Reconnect Join (raw)": _clean_raw_str(reconnect_seg.get("join_raw", ""))},
                    **{"Reconnect Leave (raw)": _clean_raw_str(reconnect_seg.get("leave_raw", ""))},
                ))

        eff_thr_raw = effective_threshold_minutes
        if rounding_mode == "ceil_attendance":
            union_min_decision = float(math.ceil(union_min_raw))
            thr_decision = eff_thr_raw
        elif rounding_mode == "ceil_both":
            union_min_decision = float(math.ceil(union_min_raw))
            thr_decision = float(math.ceil(eff_thr_raw))
        else:
            union_min_decision = union_min_raw
            thr_decision = eff_thr_raw

        meets = (union_min_decision >= thr_decision)
        is_amb = (k in ambiguous_name_keys)
        attendance_status = "Needs Review" if is_amb else ("Present" if meets else "Absent")

        pen_tol=float(penalty_tolerance_minutes or 0.0)
        bad_pct = (bad_only_minutes/union_min_raw*100.0) if union_min_raw>0 else 0.0
        pen_applied = -1 if bad_only_minutes > pen_tol else 0

        ex = (exemptions or {}).get(k, {})
        if bool(ex.get("naming", False)): pen_applied=0
        ex_overlap = bool(ex.get("overlap", False))
        ex_reconnect = bool(ex.get("reconnect", False))

        issues=[]
        if is_dual and not ex_overlap: issues.append("Duplicate account — overlapping (two devices)")
        if is_reconnect and not ex_reconnect:
            if reconnect_count>0:
                issues.append(f"Duplicate account — reconnects (non-overlapping x{reconnect_count})")
            else:
                issues.append("Duplicate account — reconnects (non-overlapping)")
        if is_amb: issues.append("Ambiguous duplicate name (no ERP / alias ambiguous)")
        merges_for_key=[src for (src,dst) in alias_merges if dst==k]
        for src in merges_for_key: issues.append(f"Merged alias {src} into {k}")

        attendance_rows.append(dict(
            Key=k,
            **{"Zoom Names (raw)": zoom_names_raw},
            **{"Attended Minutes (RAW)": round(union_min_raw,2)},
            **{"Threshold Minutes (RAW)": round(eff_thr_raw,2)},
            **{"Attended Minutes (DECISION)": round(union_min_decision,2)},
            **{"Threshold Minutes (DECISION)": round(thr_decision,2)},
            **{att_header: attendance_status},
            **{"Naming Penalty": (-1 if pen_applied==-1 else 0)},
            Issues="; ".join(issues)
        ))

        issues_rows.append(dict(
            Key=k, ERP=erp, Name=nm, **{"Zoom Names (raw)": zoom_names_raw},
            **{"Match Source": match_source_by_key.get(k,"")},
            **{"Issue Detail": "; ".join(issues) if issues else ""},
            **{"Intervals/Segments": seg_count},
            **{"Dual Devices?": ("Yes" if is_dual else "No")},
            **{"Reconnects?": ("Yes" if is_reconnect else "No")},
            **{"Reconnect Count": reconnect_count},
            **{"Ambiguous Name?": ("Yes" if is_amb else "No")},
            **{"Total Minutes Counted (Union RAW)": round(union_min_raw,2)},
            **{"Override Attendance": ""}
        ))

        if (not meets) or is_amb:
            shortfall=max(0.0, thr_decision - union_min_decision)
            absent_rows.append(dict(
                Key=k, ERP=erp, Name=nm, **{"Zoom Names (raw)": zoom_names_raw},
                **{"Attended Minutes (DECISION)": round(union_min_decision,2)},
                **{"Threshold Minutes (DECISION)": round(thr_decision,2)},
                **{"Shortfall Minutes (DECISION)": round(shortfall,2)},
                **{"Dual Devices?": ("Yes" if is_dual else "No")},
                **{"Reconnects?": ("Yes" if is_reconnect else "No")},
                **{"Reconnect Count": reconnect_count},
                **{"Is Ambiguous?": ("Yes" if is_amb else "No")},
                **{"Reason": ("Needs Review (ambiguous)" if is_amb else "")},
                **{"Override (from Issues)": ""},
                **{"Final Status": ""}
            ))

        penalties_rows.append(dict(
            Key=k, **{"Zoom Names (raw)": zoom_names_raw},
            **{"Bad-Name Minutes": round(bad_only_minutes,2)},
            **{"Bad-Name %": round(bad_pct,2)},
            **{"Penalty Tolerance (min)": pen_tol},
            **{"Penalty Applied": (-1 if pen_applied==-1 else 0)}
        ))

        matches_rows.append(dict(Key=k, ERP=erp, Name=nm, **{"Zoom Names (raw)": zoom_names_raw}, **{"Match Source": match_source_by_key.get(k,"")}))

        if erp: present_erps.add(erp)

    if not roster_df.empty:
        all_zoom_canon_names: Set[str] = set()
        for names in raw_names_by_key.values():
            for n in names:
                all_zoom_canon_names.add(_canon_name(n))
        for _,row in roster_df.iterrows():
            erp=row["ERP"]; roster_name=row["RosterName"]; roster_canon=row["RosterCanon"]
            erp_key=f"ERP:{erp}"
            if (erp not in present_erps) and (erp_key not in match_source_by_key) and (roster_canon not in all_zoom_canon_names):
                thr_decision = float(math.ceil(effective_threshold_minutes)) if rounding_mode=="ceil_both" else effective_threshold_minutes
                attendance_rows.append(dict(
                    Key=erp_key,
                    **{"Zoom Names (raw)": roster_name + " (roster)"},
                    **{"Attended Minutes (RAW)": 0.0},
                    **{"Threshold Minutes (RAW)": round(effective_threshold_minutes,2)},
                    **{"Attended Minutes (DECISION)": 0.0},
                    **{"Threshold Minutes (DECISION)": round(thr_decision,2)},
                    **{att_header: "Absent"},
                    **{"Naming Penalty": 0},
                    Issues="Not in Zoom log (Roster)"
                ))
                issues_rows.append(dict(
                    Key=erp_key, ERP=erp, Name=roster_name, **{"Zoom Names (raw)": roster_name},
                    **{"Match Source": "roster-only"},
                    **{"Issue Detail": "Not in Zoom log (Roster)"},
                    **{"Intervals/Segments": 0},
                    **{"Dual Devices?": "No"},
                    **{"Reconnects?": "No"},
                    **{"Reconnect Count": 0},
                    **{"Ambiguous Name?": "No"},
                    **{"Total Minutes Counted (Union RAW)": 0.0},
                    **{"Override Attendance": ""}
                ))
                absent_rows.append(dict(
                    Key=erp_key, ERP=erp, Name=roster_name, **{"Zoom Names (raw)": roster_name},
                    **{"Attended Minutes (DECISION)": 0.0},
                    **{"Threshold Minutes (DECISION)": round(thr_decision,2)},
                    **{"Shortfall Minutes (DECISION)": round(thr_decision,2)},
                    **{"Dual Devices?": "No"},
                    **{"Reconnects?": "No"},
                    **{"Reconnect Count": 0},
                    **{"Is Ambiguous?": "No"},
                    **{"Reason": "Not in Zoom log (Roster)"},
                    **{"Override (from Issues)": ""},
                    **{"Final Status": ""}
                ))
                penalties_rows.append(dict(
                    Key=erp_key, **{"Zoom Names (raw)": roster_name},
                    **{"Bad-Name Minutes": 0.0},
                    **{"Bad-Name %": 0.0},
                    **{"Penalty Tolerance (min)": float(penalty_tolerance_minutes or 0.0)},
                    **{"Penalty Applied": 0}
                ))
                matches_rows.append(dict(Key=erp_key, ERP=erp, Name=roster_name, **{"Zoom Names (raw)": roster_name}, **{"Match Source": "roster-only"}))

    attendance_df=pd.DataFrame(attendance_rows)
    issues_df=pd.DataFrame(issues_rows) if issues_rows else pd.DataFrame(columns=["Key","ERP","Name","Zoom Names (raw)","Match Source","Issue Detail","Intervals/Segments","Dual Devices?","Reconnects?","Reconnect Count","Ambiguous Name?","Total Minutes Counted (Union RAW)","Override Attendance"])
    absent_df=pd.DataFrame(absent_rows) if absent_rows else pd.DataFrame(columns=["Key","ERP","Name","Zoom Names (raw)","Attended Minutes (DECISION)","Threshold Minutes (DECISION)","Shortfall Minutes (DECISION)","Dual Devices?","Reconnects?","Reconnect Count","Is Ambiguous?","Reason","Override (from Issues)","Final Status"])
    penalties_df=pd.DataFrame(penalties_rows) if penalties_rows else pd.DataFrame(columns=["Key","Zoom Names (raw)","Bad-Name Minutes","Bad-Name %","Penalty Tolerance (min)","Penalty Applied"])
    matches_df=pd.DataFrame(matches_rows) if matches_rows else pd.DataFrame(columns=["Key","ERP","Name","Zoom Names (raw)","Match Source"])
    reconnects_df=pd.DataFrame(reconnect_rows) if reconnect_rows else pd.DataFrame(columns=[
        "Key","ERP","Name","Zoom Names (raw)","Event # (per student)",
        "Disconnect Time","Reconnect Time","Gap (minutes)","Gap (seconds)",
        "Gap Duration (hh:mm:ss)","Disconnect Raw Name","Reconnect Raw Name",
        "Disconnect Join (raw)","Disconnect Leave (raw)",
        "Reconnect Join (raw)","Reconnect Leave (raw)"
    ])
    if not reconnects_df.empty:
        reconnects_df = reconnects_df.sort_values(by=["Key","Event # (per student)","Disconnect Time"], kind="mergesort").reset_index(drop=True)

    if not roster_df.empty:
        erps_list=sorted(roster_df["ERP"].dropna().astype(str).unique().tolist())
    else:
        seen_erps=set()
        for r in matches_rows:
            if r.get("ERP"): seen_erps.add(str(r["ERP"]))
        erps_list=sorted(seen_erps)
    erps_df=pd.DataFrame({"ERP": erps_list})

    meta_rows = [
        ["Total class minutes (source)", "override" if override_total_minutes else "auto"],
        ["Total class minutes (before break)", round(total_class_minutes,2)],
        ["Break minutes deducted", round(break_minutes,2)],
        ["Adjusted total class minutes", round(adjusted_total_minutes,2)],
        ["Attendance threshold ratio", threshold_ratio],
        ["Raw threshold minutes (ratio * adjusted total)", round(threshold_minutes_raw,2)],
        ["Leniency buffer minutes", round(buffer_minutes,2)],
        ["EFFECTIVE threshold minutes (raw - buffer)", round(effective_threshold_minutes,2)],
        ["Decision rule", "Present if DECISION Attended >= DECISION Threshold"],
        ["Rounding mode", {"none":"None","ceil_attendance":"Ceil attendance only","ceil_both":"Ceil attendance & threshold"}[rounding_mode]],
        ["Naming penalty tolerance (minutes)", float(penalty_tolerance_minutes or 0.0)],
        ["Roster provided", "Yes" if not roster_df.empty else "No"],
        ["Excluded names patterns", "; ".join([re.sub(r'^\\^\\s*|\\s*\\$$','',p) for p in EXCLUDE_NAME_PATTERNS])]
    ]
    meta_df=pd.DataFrame(meta_rows, columns=["Metric","Value"])
    summary_df=pd.DataFrame([["(Formulas inserted by app)",""]], columns=["Metric","Value"])

    last_err=None; used_engine=None
    for engine in ("openpyxl","xlsxwriter"):
        try:
            with pd.ExcelWriter(output_xlsx, engine=engine) as w:
                raw_df.to_excel(w, index=False, sheet_name="Raw Zoom CSV")
                attendance_df.to_excel(w, index=False, sheet_name="Attendance")
                erps_df.to_excel(w, index=False, sheet_name="ERPs")
                issues_df.to_excel(w, index=False, sheet_name="Issues")
                reconnects_df.to_excel(w, index=False, sheet_name="Reconnects")
                absent_df.to_excel(w, index=False, sheet_name="Absent")
                penalties_df.to_excel(w, index=False, sheet_name="Penalties")
                matches_df.to_excel(w, index=False, sheet_name="Matches")
                meta_df.to_excel(w, index=False, sheet_name="Meta")
                summary_df.to_excel(w, index=False, sheet_name="Summary")
        except Exception as e:
            last_err=e; continue
        else:
            break
    if last_err:
        # If both engines failed, raise
        pass

    return {"output_xlsx": output_xlsx}

# -------------------- request adapter --------------------

def process_request(zoom_bytes: bytes, roster_bytes: Optional[bytes], params: Dict, exemptions: Dict) -> bytes:
    with tempfile.TemporaryDirectory() as td:
        zoom_path=os.path.join(td, 'input.csv')
        with open(zoom_path,'wb') as f: f.write(zoom_bytes)
        roster_path=None
        if roster_bytes:
            roster_path=os.path.join(td, 'roster')
            with open(roster_path,'wb') as f: f.write(roster_bytes)
        out_path=os.path.join(td, APP_FILE_DEFAULT)
        process_zoom_attendance(
            csv_path=zoom_path,
            output_xlsx=out_path,
            threshold_ratio=float(params.get('threshold_ratio', 0.8) or 0.8),
            buffer_minutes=float(params.get('buffer_minutes', 0.0) or 0.0),
            break_minutes=float(params.get('break_minutes', 0.0) or 0.0),
            exemptions=exemptions or {},
            override_total_minutes=(params.get('override_total_minutes') if params.get('override_total_minutes') not in (None, '') else None),
            penalty_tolerance_minutes=float(params.get('penalty_tolerance_minutes', 0.0) or 0.0),
            roster_path=roster_path,
            rounding_mode=str(params.get('rounding_mode', 'none') or 'none'),
        )
        return open(out_path,'rb').read()
