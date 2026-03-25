"""
analyzer_core.py – Signal JSONL Analysis Engine
"""
import json, re, math, html as _html
from collections import Counter, defaultdict
from datetime import datetime, timedelta

# ── FILTER LISTS ─────────────────────────────────────────────
GERMAN_STOPWORDS = {
    "aber","alle","als","am","an","auch","auf","aus","bei","bin","bis","bist",
    "da","dann","das","dass","dein","deine","dem","den","der","des","die",
    "doch","ein","eine","einem","einen","einer","er","es","für","haben","hat",
    "hatte","hier","ich","ihr","im","in","ist","ja","jetzt","kann","kannst",
    "man","mit","mir","mich","nicht","noch","nur","oder","sein","seine","sich",
    "sie","sind","so","über","und","uns","vom","von","vor","war","waren","was",
    "wenn","wer","wie","wir","wird","wurde","zum","zur","zu","morgen","heute",
    "halt","schon","mal","nein","ok","okay","ach","na","ne","nö","eigentlich",
    "immer","nie","irgendwie","einfach","weil","also","würde","hätte","könnte",
    "müsste","sollte","wäre","hab","habe","beim","ohne","nach","unter",
    "zwischen","nochmal","eben","gerade","trotzdem","vielleicht",
}
TECH_WORDS = {
    "https","http","www","com","de","org","net","jpeg","jpg","png","gif","webp",
    "bmp","mp4","mov","avi","mkv","webm","mp3","pdf","doc","docx","ppt","pptx",
    "xls","xlsx","image","images","img","picture","pictures","signal","file",
    "files","attachment","attachments","photo","photos","video","videos","audio",
    "document","documents","profile","avatar","group","groups","chat",
    "message","messages","application","media","sticker",
}
URL_RE  = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
_EMO_RE = re.compile(
    r"[\U0001F300-\U0001FAFF\U00002600-\U000027BF"
    r"\U0001F1E0-\U0001F1FF\U0001F900-\U0001F9FF\U00002300-\U000023FF]",
    flags=re.UNICODE,
)

# ── EMOJI EXTRACTION ─────────────────────────────────────────
_SKIN = frozenset(range(0x1F3FB, 0x1F400))
_VS16, _ZWJ, _KC = 0xFE0F, 0x200D, 0x20E3
_SKIP = frozenset(list(range(0x1F3FB,0x1F400)) +
                  [0x200D,0xFE0F,0x20E3,0x2640,0x2642,0x2695,0x2696,0x2708])

def _is_emo(cp):
    return (0x1F300<=cp<=0x1FAFF or 0x2600<=cp<=0x27BF or
            0x1F1E0<=cp<=0x1F1FF or 0x2300<=cp<=0x23FF or 0x1F900<=cp<=0x1F9FF)

def extract_emojis(text):
    result, chars, n, i = [], list(text), len(text), 0
    while i < n:
        cp = ord(chars[i])
        if not _is_emo(cp): i+=1; continue
        if cp in _SKIP:     i+=1; continue
        cluster = chars[i]; i += 1
        while i < n:
            ncp = ord(chars[i])
            if ncp in _SKIN or ncp==_VS16 or ncp==_KC:
                cluster+=chars[i]; i+=1
            elif ncp==_ZWJ and i+1<n and _is_emo(ord(chars[i+1])):
                cluster+=chars[i]+chars[i+1]; i+=2
            else: break
        result.append(cluster)
    return result

# ── HELPERS ──────────────────────────────────────────────────
def safe_loads(line):
    try: return json.loads(line)
    except: return None

def get_nested(obj, path, default=None):
    cur = obj
    for k in path:
        if not isinstance(cur, dict) or k not in cur: return default
        cur = cur[k]
    return cur

def first_existing(obj, paths, default=None):
    for p in paths:
        v = get_nested(obj, p, None)
        if v is not None: return v
    return default

def is_noise(t):
    s = str(t).strip()
    return (not s or bool(re.fullmatch(r"[A-Za-z0-9+/=]{20,}",s))
            or bool(re.fullmatch(r"\d{6,}",s)))

esc  = lambda s: _html.escape(str(s), quote=True)
avg  = lambda l: sum(l)/len(l) if l else 0.0
pct  = lambda p,t: 100.0*p/t if t else 0.0
nfmt = lambda n: f"{int(n):,}".replace(",",".")

def fmt_time(m):
    if m < 1:   return "<1 Min"
    if m < 60:  return f"{m:.0f} Min"
    return f"{m/60:.1f} Std"

WDAY = {"Monday":"Montag","Tuesday":"Dienstag","Wednesday":"Mittwoch",
        "Thursday":"Donnerstag","Friday":"Freitag","Saturday":"Samstag","Sunday":"Sonntag"}

def day_de(s): return WDAY[datetime.strptime(s,"%Y-%m-%d").strftime("%A")]

def streak(ds):
    if not ds: return 0,None,None
    bl,bs,be=1,ds[0],ds[0]; cs,cl=ds[0],1
    for p,c in zip(ds,ds[1:]):
        if c==p+timedelta(days=1): cl+=1
        else:
            if cl>bl: bl,bs,be=cl,cs,p
            cs,cl=c,1
    if cl>bl: bl,bs,be=cl,cs,ds[-1]
    return bl,bs,be

def tokenize(text):
    return [w for w in re.findall(r"\b\w+\b",URL_RE.sub(" ",text).lower(),flags=re.UNICODE)
            if len(w)>2 and w not in GERMAN_STOPWORDS and w not in TECH_WORDS
            and not re.fullmatch(r"\d+",w)]

# ── PARSE EXPORT ─────────────────────────────────────────────
def parse_export(file_bytes):
    account_name = None
    chats = []; raw_contacts = {}
    lines = file_bytes.decode("utf-8", errors="replace").splitlines()

    for line in lines:
        item = safe_loads(line)
        if not item: continue

        if "account" in item and isinstance(item["account"], dict):
            acc = item["account"]
            if not account_name:
                account_name = acc.get("givenName") or acc.get("username") or "Ich"

        r = item.get("recipient")
        if not isinstance(r, dict): continue
        cid = str(r.get("id","")).strip()
        if not cid: continue

        if "contact" in r:
            c = r["contact"]
            profile=(c.get("profileGivenName") or "").strip()
            system =(c.get("systemGivenName")  or "").strip()
            nick   =(get_nested(c,("nickname","given"),"") or "").strip()
            aci    =(c.get("aci")  or "").strip()
            e164   =(c.get("e164") or "").strip()
            pni    =(c.get("pni")  or "").strip()
            best   = profile or system or nick or e164 or cid
            all_ids= [x for x in [aci,e164,pni,cid] if x]
            chats.append({"chat_id":cid,"type":"contact","display_name":best,
                          "_ids":all_ids,"profile_name":profile,
                          "system_name":system,"nickname":nick})
        elif "group" in r:
            g = r["group"]
            title=(get_nested(g,("snapshot","title","title"),"") or "").strip()
            chats.append({"chat_id":cid,"type":"group",
                          "title":title or f"Gruppe {cid}"})

    # Collect active chat recipient IDs from "chat" entries
    # chatId in chatItems = chat.id, which maps to recipientId
    # Build: chat_id_to_recipient_id and active_recipient_ids
    active_recipient_ids = set()
    chat_id_to_recipient = {}  # chat.id -> recipient.id (usually same)
    for line in lines:
        item = safe_loads(line)
        if not item: continue
        ch = item.get("chat")
        if isinstance(ch, dict):
            cid = str(ch.get("id","")).strip()
            rid = str(ch.get("recipientId","")).strip()
            if cid and rid:
                chat_id_to_recipient[cid] = rid
                active_recipient_ids.add(rid)

    # Deduplicate recipients
    seen = {}
    for c in chats:
        k = c["chat_id"]
        if k not in seen: seen[k] = c
        elif c.get("display_name") and not seen[k].get("display_name"): seen[k] = c

    # Only keep recipients that have an active chat entry
    if active_recipient_ids:
        chats = [c for c in seen.values() if c["chat_id"] in active_recipient_ids]
    else:
        chats = list(seen.values())  # fallback: show all

    # Number duplicate display names
    contact_chats = [c for c in chats if c.get("type")=="contact"]
    name_count = Counter(c["display_name"] for c in contact_chats)
    name_idx   = {}
    for c in contact_chats:
        n = c["display_name"]
        if name_count[n] > 1:
            name_idx[n] = name_idx.get(n,0)+1
            c["display_name"] = f"{n} {name_idx[n]}"
        for id_val in c.get("_ids",[]):
            if id_val: raw_contacts[id_val] = c["display_name"]

    return account_name or "Ich", chats, raw_contacts, chat_id_to_recipient

def get_chat_list(file_bytes):
    _, chats, _, _ctr = parse_export(file_bytes)
    result = []
    for c in chats:
        if c["type"] == "contact":
            result.append({"chat_id":c["chat_id"],"type":"contact","name":c.get("display_name","?")})
        else:
            result.append({"chat_id":c["chat_id"],"type":"group","name":c.get("title","?")})
    return sorted(result, key=lambda x:(x["type"],x["name"].lower()))

# ── SENDER RESOLVER ───────────────────────────────────────────
class SenderResolver:
    def __init__(self, target, self_name, raw_contacts):
        self.target=target; self.self_name=self_name
        self.raw=raw_contacts; self._n=0; self._map={}
    def resolve(self, entry, sid):
        if "outgoing" in entry: return self.self_name
        if self.target.get("type") != "group":
            return self.target.get("display_name") or "Kontakt"
        if sid:
            if sid in self.raw: return self.raw[sid]
            if sid not in self._map: self._n+=1; self._map[sid]=f"Unbekannt {self._n}"
            return self._map[sid]
        return "Unbekannt"

def get_text(entry):
    body=get_nested(entry,("standardMessage","text","body"))
    if isinstance(body,str) and body.strip(): return body.strip()
    body=get_nested(entry,("chatItem","standardMessage","text","body"))
    if isinstance(body,str) and body.strip(): return body.strip()
    for p in [("text","body"),("message","text","body"),("body",),("content",),("caption",)]:
        v=get_nested(entry,p)
        if isinstance(v,str) and v.strip() and not is_noise(v): return v.strip()
    return ""

def get_ts(item):
    for p in [("dateSent",),("dateReceived",),("timestamp",),
              ("standardMessage","timestamp"),("incoming","dateReceived"),("outgoing","dateReceived")]:
        v=get_nested(item,p)
        if v is None: continue
        try: return int(v)
        except: pass
    return None

def get_sid(item):
    for k in ("authorId","senderId","source","sender"):
        v=item.get(k)
        if isinstance(v,str) and v: return v
    return None

def is_msg(entry):
    if not isinstance(entry,dict): return False
    if "updateMessage" in entry: return False
    return any(k in entry for k in
        ["standardMessage","incoming","outgoing","reaction","callMessage","message","body"])

# ── ANALYTICS ────────────────────────────────────────────────
def word_development(sorted_msgs):
    n=len(sorted_msgs)
    if n<60: return [],[]
    third=n//3
    parts=[sorted_msgs[:third],sorted_msgs[third:2*third],sorted_msgs[2*third:]]
    def freq(msgs):
        c=Counter()
        for m in msgs: c.update(tokenize(m["text"]))
        total=sum(c.values()) or 1
        return {w:v/total for w,v in c.items()}, sum(c.values())
    f1,t1=freq(parts[0]); f3,t3=freq(parts[2])
    rising=[]; falling=[]
    for w in set(f1)|set(f3):
        v1=f1.get(w,0); v3=f3.get(w,0)
        c1=round(v1*t1); c3=round(v3*t3)
        if c1+c3<5: continue
        if v1>0 and v3>0:
            ratio=v3/v1
            if ratio>2.5 and c3>=4: rising.append((w,ratio,c3))
            if ratio<0.35 and c1>=4: falling.append((w,ratio,c1))
        elif v1==0 and c3>=6: rising.append((w,99.0,c3))
        elif v3==0 and c1>=6: falling.append((w,0.0,c1))
    return sorted(rising,key=lambda x:-x[1])[:8], sorted(falling,key=lambda x:x[1])[:8]

def word_trends(sorted_msgs):
    if not sorted_msgs: return []
    ts_vals=[m["ts"] for m in sorted_msgs if m["ts"]]
    if not ts_vals: return []
    last_ts=max(ts_vals); cutoff=last_ts-60*24*60*60*1000
    recent =[m for m in sorted_msgs if m["ts"] and m["ts"]>=cutoff]
    earlier=[m for m in sorted_msgs if not m["ts"] or m["ts"]<cutoff]
    if len(recent)<20: return []
    def fc(msgs):
        c=Counter()
        for m in msgs: c.update(tokenize(m["text"]))
        return c, sum(c.values()) or 1
    cr,tr=fc(recent); ce,te=fc(earlier)
    gags=[]
    for w,cnt in cr.most_common(200):
        if cnt<5: break
        base=ce.get(w,0)/te; cur=cnt/tr
        if base==0: base=0.5/te
        spike=cur/base
        if spike>=3.0 and cnt>=5: gags.append((w,spike,cnt))
    return sorted(gags,key=lambda x:-x[1])[:8]

def sprecherwechsel(msgs):
    if len(msgs)<2: return 0.0
    changes=sum(1 for a,b in zip(msgs,msgs[1:]) if a["sender"]!=b["sender"])
    return changes/(len(msgs)-1)*100

# ── DESIGN TOKENS ─────────────────────────────────────────────
BG="#0a0a0a"; CARD="#111111"; BD="#1e1e1e"
MUT="#555555"; TXT="#cccccc"; WHT="#ffffff"
GOLD="#c9a84c"; BLUE="#6b9fff"; GRN="#6bcc88"; RED="#ff6b6b"
MONO="'JetBrains Mono','Fira Mono',monospace"

# ── HTML BUILDERS ─────────────────────────────────────────────
def stat_card(label,value,sub=""):
    sub_h=(f'<div style="color:{MUT};font-size:.72rem;margin-top:4px;font-family:{MONO};line-height:1.4">{esc(sub)}</div>') if sub else ""
    return (f'<div style="background:{CARD};border:1px solid {BD};border-radius:10px;padding:18px 16px 14px">'
            f'<div style="color:{MUT};font-size:.7rem;text-transform:uppercase;letter-spacing:.09em">{esc(label)}</div>'
            f'<div style="font-size:1.75rem;font-weight:800;color:{WHT};margin-top:6px;line-height:1.1;word-break:break-word">{esc(str(value))}</div>'
            f'{sub_h}</div>')

def card_grid(*cards):
    return (f'<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(155px,1fr));gap:12px;margin-bottom:24px">'
            +"".join(cards)+'</div>')

def section(title,body,accent=GOLD):
    return (f'<div style="margin-bottom:28px">'
            f'<div style="display:flex;align-items:center;gap:10px;margin-bottom:12px">'
            f'<div style="width:3px;height:16px;background:{accent};border-radius:2px;flex-shrink:0"></div>'
            f'<div style="font-size:.7rem;text-transform:uppercase;letter-spacing:.12em;color:{MUT};font-weight:600">{esc(title)}</div>'
            f'</div><div style="background:{CARD};border:1px solid {BD};border-radius:12px;padding:20px 18px">'
            f'{body}</div></div>')

def bar_row(label,value,max_val,accent=GOLD,suffix=""):
    pw=max(3,int(value/max_val*100)) if max_val else 3
    return (f'<div style="display:flex;align-items:center;gap:12px;padding:5px 0;border-bottom:1px solid {BD}">'
            f'<div style="width:185px;min-width:185px;font-size:.88rem;color:{TXT};overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{esc(label)}</div>'
            f'<div style="flex:1;height:6px;background:#161616;border-radius:3px;overflow:hidden">'
            f'<div style="height:6px;width:{pw}%;background:{accent};border-radius:3px"></div></div>'
            f'<div style="min-width:90px;text-align:right;font-family:{MONO};font-size:.82rem;color:{MUT}">{nfmt(value)}{esc(suffix)}</div>'
            f'</div>')

def emoji_bar_row(emo,value,max_val):
    pw=max(3,int(value/max_val*100)) if max_val else 3
    return (f'<div style="display:flex;align-items:center;gap:12px;padding:6px 0;border-bottom:1px solid {BD}">'
            f'<div style="width:36px;min-width:36px;font-size:1.3rem;line-height:1;text-align:center">{emo}</div>'
            f'<div style="flex:1;height:6px;background:#161616;border-radius:3px;overflow:hidden">'
            f'<div style="height:6px;width:{pw}%;background:{GOLD};border-radius:3px"></div></div>'
            f'<div style="min-width:44px;text-align:right;font-family:{MONO};font-size:.82rem;color:{MUT}">{nfmt(value)}</div>'
            f'</div>')

def divider():
    return f'<div style="height:1px;background:{BD};margin:16px 0"></div>'

def sub_label(text,accent=MUT):
    return (f'<div style="font-size:.68rem;text-transform:uppercase;letter-spacing:.1em;'
            f'color:{accent};margin:14px 0 6px;font-weight:600">{esc(text)}</div>')

def response_profile_bar(resp_times):
    if not resp_times: return ""
    bucket_defs=[
        ("Sofort",    "unter 1 Minute",     BLUE,      lambda t:t<1),
        ("Schnell",   "1–10 Minuten",        GRN,       lambda t:t<10),
        ("Normal",    "10–60 Minuten",       GOLD,      lambda t:t<60),
        ("Spät",      "1–24 Stunden",        "#ff9e64", lambda t:t<1440),
        ("Sehr spät", "mehr als 24 Stunden", RED,       lambda t:True),
    ]
    buckets={d[0]:0 for d in bucket_defs}
    for t in resp_times:
        for label,desc,color,check in bucket_defs:
            if check(t): buckets[label]+=1; break
    total=sum(buckets.values()) or 1
    segs=""; legend_parts=[]
    for label,desc,color,_ in bucket_defs:
        cnt=buckets[label]
        if cnt==0: continue
        p=pct(cnt,total)
        segs+=(f'<div style="flex:{p:.1f};background:{color};height:8px;min-width:4px;border-radius:1px" '
               f'title="{label} ({desc}): {cnt} ({p:.0f}%)"></div>')
        legend_parts.append(f'<span style="color:{color};font-size:.72rem">{label}</span> '
                            f'<span style="color:{MUT};font-size:.7rem">= {desc}</span>')
    legend=" &nbsp;·&nbsp; ".join(legend_parts)
    return (f'<div style="display:flex;border-radius:4px;overflow:hidden;margin:8px 0 2px">{segs}</div>'
            f'<div style="font-family:{MONO};margin-bottom:4px">{legend}</div>')

def person_card(name,pd,total_msgs,avg_resp_val,resp_times):
    count=pd["count"]; p=pct(count,total_msgs)
    av_ch=avg(pd["chars"]); av_wo=avg(pd["word_lens"])
    er=pct(pd["emoji_n"],count); qr=pct(pd["q_n"],count)
    top5=", ".join(w for w,_ in pd["words"].most_common(5)) or "–"
    resp_str=fmt_time(avg_resp_val) if avg_resp_val else "–"
    mini=(f'<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:8px;margin:12px 0 10px">'
          +stat_card("Nachrichten",nfmt(count),f"{p:.1f}% des Chats")
          +stat_card("Ø Zeichen",f"{av_ch:.0f}","pro Nachricht")
          +stat_card("Ø Wörter",f"{av_wo:.1f}","pro Nachricht")
          +stat_card("Emoji-Rate",f"{er:.1f}%","mit Emoji")
          +stat_card("Fragen",f"{qr:.1f}%","mit ?")
          +stat_card("Ø Antwortzeit",resp_str,"auf andere Nachrichten")
          +'</div>'
          +sub_label("Lieblingswörter")
          +f'<div style="font-size:.85rem;color:{TXT};margin-bottom:8px">{esc(top5)}</div>'
          +sub_label("Antwort-Profil")
          +response_profile_bar(resp_times))
    return (f'<details style="margin-bottom:8px">'
            f'<summary style="display:flex;align-items:center;gap:12px;padding:12px 16px;'
            f'cursor:pointer;list-style:none;border-radius:12px;background:{CARD};border:1px solid {BD}">'
            f'<span style="font-weight:700;color:{WHT};font-size:.95rem">{esc(name)}</span>'
            f'<span style="color:{MUT};font-family:{MONO};font-size:.78rem;margin-left:auto">'
            f'{nfmt(count)} · {p:.1f}%</span></summary>'
            f'<div style="background:{CARD};border:1px solid {BD};border-top:none;'
            f'border-radius:0 0 12px 12px;padding:4px 16px 16px">{mini}</div></details>')

def monthly_chart_svg(day_counts):
    if not day_counts: return ""
    mc=Counter()
    for dk,c in day_counts.items(): mc[dk[:7]]+=c
    months=sorted(mc)
    if not months: return ""
    if len(months)>24: months=months[-24:]
    vals=[mc[m] for m in months]; max_v=max(vals) or 1
    W,H=680,160; pl=32; pb=28; pt=14; pr=12
    cw=W-pl-pr; ch=H-pt-pb; n=len(months)
    bw=max(4,cw/n*0.72); gap=cw/n
    def ma(vs,w=3):
        out=[]
        for i in range(len(vs)):
            s=max(0,i-w+1); out.append(avg(vs[s:i+1]))
        return out
    mavg=ma(vals,3); bars=""; last_yr=None
    for i,(m,v) in enumerate(zip(months,vals)):
        x=pl+i*gap+gap/2-bw/2; bh=(v/max_v)*ch; y=pt+ch-bh; frac=v/max_v
        bars+=(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bw:.1f}" height="{bh:.1f}" '
               f'rx="2" fill="{GOLD}" opacity="{0.45+0.55*frac:.2f}"/>')
        yr=m[:4]
        if yr!=last_yr or i==0:
            last_yr=yr; lx=pl+i*gap+gap/2
            bars+=(f'<text x="{lx:.1f}" y="{H-4}" font-family="JetBrains Mono,monospace" '
                   f'font-size="9" fill="{MUT}" text-anchor="middle">{m[5:]}/{m[2:4]}</text>')
    pts=[f"{pl+i*gap+gap/2:.1f},{pt+ch-(v/max_v)*ch:.1f}" for i,v in enumerate(mavg)]
    trend=f'<polyline points="{" ".join(pts)}" fill="none" stroke="{BLUE}" stroke-width="1.5" stroke-linejoin="round"/>'
    yl=""
    for frac in [0.25,0.5,0.75,1.0]:
        val=int(max_v*frac); ly=pt+ch-frac*ch
        yl+=(f'<line x1="{pl}" y1="{ly:.1f}" x2="{W-pr}" y2="{ly:.1f}" stroke="{BD}" stroke-width="1"/>'
             f'<text x="{pl-4}" y="{ly+3:.1f}" font-family="JetBrains Mono,monospace" '
             f'font-size="9" fill="{MUT}" text-anchor="end">{val}</text>')
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" '
            f'style="width:100%;height:auto;display:block;overflow:visible">'
            f'{yl}{bars}{trend}'
            f'<line x1="{pl}" y1="{H-pb}" x2="{W-pr}" y2="{H-pb}" stroke="{BD}" stroke-width="1"/>'
            f'</svg>')

def heatmap_svg(day_counts):
    if not day_counts: return ""
    dl=sorted(day_counts)
    start=datetime.strptime(dl[0],"%Y-%m-%d").date()
    end  =datetime.strptime(dl[-1],"%Y-%m-%d").date()
    s_mon=start-timedelta(days=start.weekday())
    e_sun=end  +timedelta(days=6-end.weekday())
    max_c=max(day_counts.values()) or 1
    def color(c):
        if c==0: return "#141414"
        p=c/max_c
        if p<.20: return "#1e3a20"
        if p<.45: return "#2e6830"
        if p<.75: return "#4a9840"
        return GRN
    CS=11; G=2; S=CS+G
    weeks=[]; d=s_mon
    while d<=e_sun:
        week=[(d+timedelta(days=i),day_counts.get((d+timedelta(days=i)).strftime("%Y-%m-%d"),0))
              for i in range(7)]
        weeks.append(week); d+=timedelta(weeks=1)
    n=len(weeks); PL=28; PT=22; PB=8
    W=PL+n*S+4; H=PT+7*S+PB
    cells=""; labels=""; last_m=None
    for wi,week in enumerate(weeks):
        m=week[0][0].month
        if m!=last_m:
            last_m=m; lx=PL+wi*S
            labels+=(f'<text x="{lx}" y="{PT-6}" font-family="JetBrains Mono,monospace" '
                     f'font-size="9" fill="{MUT}">{week[0][0].strftime("%b")}</text>')
        for di,(dd,cnt) in enumerate(week):
            x=PL+wi*S; y=PT+di*S
            cells+=(f'<rect x="{x}" y="{y}" width="{CS}" height="{CS}" rx="2" fill="{color(cnt)}">'
                    f'<title>{dd.strftime("%d.%m.%Y")}: {cnt}</title></rect>')
    for di,name in enumerate(["Mo","","Mi","","Fr","",""]):
        if not name: continue
        y=PT+di*S+CS//2+3
        labels+=(f'<text x="{PL-4}" y="{y}" font-family="JetBrains Mono,monospace" '
                 f'font-size="9" fill="{MUT}" text-anchor="end">{name}</text>')
    return (f'<div style="overflow-x:auto;padding-bottom:4px">'
            f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" '
            f'style="min-width:{W}px;height:{H}px;display:block">'
            f'{labels}{cells}</svg></div>')

def score_svg(score,mpd,cons_pct):
    sc=GRN if score>=70 else (GOLD if score>=40 else RED)
    R=44; CX=CY=52; sw=7
    cf=2*math.pi*R; dash=cf*score/100
    ring=(f'<circle cx="{CX}" cy="{CY}" r="{R}" fill="none" stroke="{BD}" stroke-width="{sw}"/>'
          f'<circle cx="{CX}" cy="{CY}" r="{R}" fill="none" stroke="{sc}" stroke-width="{sw}" '
          f'stroke-linecap="round" stroke-dasharray="{dash:.2f} {cf:.2f}" transform="rotate(-90 {CX} {CY})"/>'
          f'<text x="{CX}" y="{CY+5}" text-anchor="middle" '
          f'font-family="Bricolage Grotesque,sans-serif" font-size="22" font-weight="800" fill="{sc}">{score}</text>'
          f'<text x="{CX}" y="{CY+19}" text-anchor="middle" '
          f'font-family="JetBrains Mono,monospace" font-size="8" fill="{MUT}">von 100</text>')
    svg=f'<svg xmlns="http://www.w3.org/2000/svg" width="104" height="104" style="flex-shrink:0">{ring}</svg>'
    def mini2(label,val,sub=""):
        return (f'<div style="background:#0d0d0d;border:1px solid {BD};border-radius:8px;padding:12px">'
                f'<div style="color:{MUT};font-size:.68rem;text-transform:uppercase;letter-spacing:.08em">{esc(label)}</div>'
                f'<div style="font-size:1.3rem;font-weight:700;color:{WHT};margin-top:4px">{esc(val)}</div>'
                +(f'<div style="color:{MUT};font-size:.7rem;margin-top:2px">{esc(sub)}</div>' if sub else "")
                +'</div>')
    details=(f'<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;flex:1;min-width:200px">'
             +mini2("Intensität",f"{mpd:.1f} / Tag","Nachrichten pro aktivem Tag")
             +mini2("Konstanz",f"{cons_pct:.0f}%","Anteil aktiver Tage")
             +'</div>')
    return (f'<div style="display:flex;align-items:center;gap:20px;flex-wrap:wrap">'
            f'{svg}{details}</div>')

def wdev_col(items,arrow,color):
    if not items: return f"<p style='color:{MUT};font-size:.85rem'>Zu wenig Daten.</p>"
    m=items[0][2]; rows=""
    for w,ratio,cnt in items:
        pw=max(4,int(cnt/m*100)); rat=f"{ratio:.1f}×" if ratio<90 else "neu"
        rows+=(f'<div style="display:flex;align-items:center;gap:8px;padding:4px 0;border-bottom:1px solid {BD}">'
               f'<div style="width:90px;min-width:0;font-size:.85rem;color:{TXT};overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{arrow} {esc(w)}</div>'
               f'<div style="flex:1;height:5px;background:#161616;border-radius:2px;overflow:hidden">'
               f'<div style="height:5px;width:{pw}%;background:{color};border-radius:2px"></div></div>'
               f'<div style="min-width:60px;text-align:right;font-family:{MONO};font-size:.78rem;color:{MUT}">{rat} · {cnt}×</div>'
               f'</div>')
    return rows

# ── MAIN ANALYSIS ─────────────────────────────────────────────
def run_analysis(file_bytes, chat_id):
    self_name, chats, raw_contacts, chat_id_to_recipient = parse_export(file_bytes)
    target = next((c for c in chats if c["chat_id"]==chat_id), None)
    if not target: return "<p>Chat nicht gefunden.</p>"

    chat_name = (target.get("display_name") if target["type"]=="contact"
                 else target.get("title")) or "Chat"
    resolver  = SenderResolver(target, self_name, raw_contacts)
    lines     = file_bytes.decode("utf-8",errors="replace").splitlines()

    messages=[]; sender_counts=Counter(); day_counts=Counter()
    hour_counts=Counter({h:0 for h in range(24)})
    weekday_counts=Counter({"Montag":0,"Dienstag":0,"Mittwoch":0,
                            "Donnerstag":0,"Freitag":0,"Samstag":0,"Sonntag":0})
    word_counts=Counter(); emoji_counts=Counter(); bigram_counts=Counter()
    q_msgs=emo_msgs=night_msgs=0; lengths_chars=[]
    person_data=defaultdict(lambda:{"count":0,"chars":[],"word_lens":[],"emoji_n":0,"q_n":0,"words":Counter()})
    _last_s=None; _cur_b=0; serie_counts=Counter()

    for idx,line in enumerate(lines):
        item=safe_loads(line)
        if not item: continue
        entry=item.get("chatItem",item)
        if not is_msg(entry): continue
        cid=first_existing(entry,[("chatId",),("recipient","id"),("conversationId",),("threadId",)],None)
        if cid is None: continue
        # Translate chat.id → recipient.id if needed (usually same, but be safe)
        resolved_cid = chat_id_to_recipient.get(str(cid), str(cid))
        if resolved_cid != chat_id: continue

        text=get_text(entry); ts=get_ts(entry); sid=get_sid(entry)
        sender=resolver.resolve(entry,sid)
        sender_counts[sender]+=1
        pd=person_data[sender]; pd["count"]+=1

        if sender!=_last_s:
            if _last_s and _cur_b>=3: serie_counts[_last_s]+=1
            _last_s=sender; _cur_b=1
        else: _cur_b+=1

        if not text: continue
        when_str=""
        if ts:
            try: when_str=datetime.fromtimestamp(ts/1000).strftime("%d.%m. %H:%M")
            except: pass

        messages.append({"text":text,"ts":ts,"sender":sender,"order":idx,"when":when_str})
        lengths_chars.append(len(text))
        tokens=tokenize(text)
        pd["chars"].append(len(text)); pd["word_lens"].append(len(tokens))
        pd["words"].update(tokens)
        if _EMO_RE.search(text): pd["emoji_n"]+=1; emo_msgs+=1
        if "?" in text: pd["q_n"]+=1; q_msgs+=1

        if ts:
            try:
                dt=datetime.fromtimestamp(ts/1000); dk=dt.strftime("%Y-%m-%d")
                day_counts[dk]+=1; hour_counts[dt.hour]+=1
                weekday_counts[day_de(dk)]+=1
                if dt.hour>=22 or dt.hour<5: night_msgs+=1
            except: pass

        word_counts.update(tokens)
        if len(tokens)>=2: bigram_counts.update(zip(tokens,tokens[1:]))
        emoji_counts.update(extract_emojis(text))

    if _last_s and _cur_b>=3: serie_counts[_last_s]+=1

    # ── Stats ─────────────────────────────────────────────────
    total=len(messages); act_days=len(day_counts)
    av_chars=avg(lengths_chars)
    dates_s=sorted(datetime.strptime(d,"%Y-%m-%d").date() for d in day_counts)
    total_days=(dates_s[-1]-dates_s[0]).days+1 if len(dates_s)>=2 else 1
    mpd=total/act_days if act_days else 0.0; cons_pct=pct(act_days,total_days)
    streak_d=[datetime.combine(d,datetime.min.time()) for d in dates_s]
    stl,sts,ste=streak(streak_d)
    max_gap=max(((b-a).days for a,b in zip(dates_s,dates_s[1:])),default=0)
    mad=day_counts.most_common(1)[0] if day_counts else ("-",0)
    mah=hour_counts.most_common(1)[0] if hour_counts else (0,0)
    maw=weekday_counts.most_common(1)[0] if weekday_counts else ("-",0)
    quiet_w=min(weekday_counts,key=weekday_counts.get) if weekday_counts else "-"
    mcw=word_counts.most_common(1)[0][0] if word_counts else "-"
    mcwn=word_counts.most_common(1)[0][1] if word_counts else 0
    tbig=bigram_counts.most_common(1)[0][0] if bigram_counts else ("-","-")
    tphr=" ".join(tbig)
    te=emoji_counts.most_common(1)[0][0] if emoji_counts else "-"
    ten=emoji_counts.most_common(1)[0][1] if emoji_counts else 0
    uw=len(word_counts); twt=sum(word_counts.values())
    wdiv=uw/twt if twt else 0.0
    date_range=(f"{dates_s[0].strftime('%d.%m.%Y')} – {dates_s[-1].strftime('%d.%m.%Y')}"
                if dates_s else "–")
    streak_sub=(f"{sts.date().strftime('%d.%m.%Y')} → {ste.date().strftime('%d.%m.%Y')}"
                if sts and ste else "")

    sorted_msgs=sorted(messages,key=lambda m:(m["ts"] if m["ts"] else 10**18,m["order"]))

    trend_label="–"
    if len(dates_s)>=6:
        month_c=Counter()
        for dk,c in day_counts.items(): month_c[dk[:7]]+=c
        ms=sorted(month_c); half=len(ms)//2
        if half>0:
            r1=avg([month_c[m] for m in ms[:half]]); r2=avg([month_c[m] for m in ms[half:]])
            if r2>r1*1.2:  trend_label="↑ Wird aktiver"
            elif r2<r1*0.8: trend_label="↓ Wird ruhiger"
            else:            trend_label="→ Stabil"

    act_score=int(min(1.0,mpd/40)*50+(cons_pct/100)*50)

    resp_times_per=defaultdict(list); initiator_cnt=Counter()
    prev_ts_r=prev_sender_r=prev_ts_i=None
    for msg in sorted_msgs:
        ts_m=msg["ts"]
        if prev_sender_r and msg["sender"]!=prev_sender_r and ts_m and prev_ts_r:
            delta=(ts_m-prev_ts_r)/60000
            if 0<delta<1440: resp_times_per[msg["sender"]].append(delta)
        prev_sender_r=msg["sender"]; prev_ts_r=ts_m
        if ts_m:
            if prev_ts_i is None or (ts_m-prev_ts_i)/60000>180: initiator_cnt[msg["sender"]]+=1
            prev_ts_i=ts_m

    avg_resp={s:avg(ts) for s,ts in resp_times_per.items() if len(ts)>=8}
    rising,falling=word_development(sorted_msgs)
    gags=word_trends(sorted_msgs)
    sw_idx=sprecherwechsel(sorted_msgs)

    # ── Build HTML ────────────────────────────────────────────
    wd_ord=["Montag","Dienstag","Mittwoch","Donnerstag","Freitag","Samstag","Sonntag"]
    hr_max=max(hour_counts.values()) if hour_counts else 1
    wk_max=max(weekday_counts.values()) if weekday_counts else 1
    d20_max=day_counts.most_common(1)[0][1] if day_counts else 1
    s_max=max(sender_counts.values()) if sender_counts else 1
    i_total=sum(initiator_cnt.values()) or 1; i_max=max(initiator_cnt.values()) if initiator_cnt else 1
    tw_items=word_counts.most_common(20); tw_max=tw_items[0][1] if tw_items else 1
    te_items=emoji_counts.most_common(20); te_max=te_items[0][1] if te_items else 1

    sender_bars="".join(bar_row(s,c,s_max,GOLD,f"  ({pct(c,total):.1f}%)") for s,c in sender_counts.most_common())
    init_bars="".join(bar_row(s,c,i_max,BLUE,f"  ({pct(c,i_total):.0f}%)") for s,c in initiator_cnt.most_common())
    r_sorted=sorted(avg_resp.items(),key=lambda x:x[1]); r_max=max(avg_resp.values()) if avg_resp else 1
    resp_bars=("".join(bar_row(s,round(t),r_max,GRN,f"  ({fmt_time(t)})") for s,t in r_sorted)
               if r_sorted else f"<p style='color:{MUT};font-size:.88rem'>Zu wenig Daten (mind. 8 Antworten/Person).</p>")
    p_details="".join(person_card(name,person_data[name],total,avg_resp.get(name),resp_times_per.get(name,[]))
                      for name,_ in sender_counts.most_common())
    top_words_html="".join(bar_row(w,c,tw_max,BLUE) for w,c in tw_items)
    top_emo_html=("".join(emoji_bar_row(e,c,te_max) for e,c in te_items)
                  or f"<p style='color:{MUT};font-size:.88rem'>Keine Emojis gefunden.</p>")
    wdev_html=(f'<div style="font-size:.78rem;color:{MUT};margin-bottom:14px">Vergleich: erste ⅓ vs. letzte ⅓</div>'
               f'<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));'
               f'gap:16px;overflow:hidden">'
               f'<div style="min-width:0;overflow:hidden">'+sub_label("Aufsteigend",GRN)+wdev_col(rising,"↑",GRN)+'</div>'
               f'<div style="min-width:0;overflow:hidden">'+sub_label("Absteigend",RED)+wdev_col(falling,"↓",RED)+'</div></div>')
    gag_max=gags[0][2] if gags else 1
    gag_html=("".join(
        f'<div style="display:flex;align-items:center;gap:10px;padding:5px 0;border-bottom:1px solid {BD}">'
        f'<div style="width:160px;font-size:.88rem;color:{TXT}">{esc(w)}</div>'
        f'<div style="flex:1;height:6px;background:#161616;border-radius:3px;overflow:hidden">'
        f'<div style="height:6px;width:{max(4,int(cnt/gag_max*100))}%;background:{GOLD};border-radius:3px"></div></div>'
        f'<div style="min-width:120px;text-align:right;font-family:{MONO};font-size:.78rem;color:{MUT}">'
        f'{spike:.1f}× Spike · {cnt}×</div></div>'
        for w,spike,cnt in gags)
        or f"<p style='color:{MUT};font-size:.88rem'>Mind. 60 Tage Verlauf nötig.</p>")
    wv_html=(f'<div style="display:flex;align-items:flex-end;gap:16px;flex-wrap:wrap">'
             f'<div><div style="color:{MUT};font-size:.7rem;text-transform:uppercase;letter-spacing:.09em">Score</div>'
             f'<div style="font-size:3rem;font-weight:800;color:{GOLD};line-height:1">{wdiv:.3f}</div>'
             f'<div style="color:{MUT};font-size:.78rem;margin-top:4px">0 = immer gleiche Wörter &nbsp;·&nbsp; 1 = jedes Wort einmalig</div></div>'
             f'<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;flex:1;min-width:250px">'
             +stat_card("Einzigartige Wörter",nfmt(uw),f"von {nfmt(twt)} gesamt")
             +stat_card("Nur 1× genutzt",nfmt(sum(1 for c in word_counts.values() if c==1)),"Wörter")
             +'</div></div>')

    main_cards=card_grid(
        stat_card("Nachrichten",nfmt(total),f"Ø {mpd:.1f} pro Tag"),
        stat_card("Aktive Tage",nfmt(act_days),date_range),
        stat_card("Ø Länge",f"{av_chars:.0f} Zeichen","pro Nachricht"),
        stat_card("Emoji-Nachrichten",nfmt(emo_msgs),f"{pct(emo_msgs,total):.1f}%"),
        stat_card("Fragen",nfmt(q_msgs),f"{pct(q_msgs,total):.1f}%"),
        stat_card("Längste Serie",f"{stl} Tage",streak_sub),
    )
    hl_cards=card_grid(
        stat_card("Aktivster Tag",mad[0],f"{mad[1]:,} Nachrichten"),
        stat_card("Aktivste Stunde",f"{mah[0]:02d}:00",f"{mah[1]:,} Nachrichten"),
        stat_card("Aktivster Wochentag",maw[0],f"{maw[1]:,} Nachrichten"),
        stat_card("Ruhigster Wochentag",quiet_w,f"{weekday_counts.get(quiet_w,0):,} Nachrichten"),
        stat_card("Nacht-Nachrichten",nfmt(night_msgs),f"{pct(night_msgs,total):.1f}% (22–05 Uhr)"),
        stat_card("Größte Pause",f"{max_gap} Tage","ohne Nachricht"),
        stat_card("Häufigstes Wort",mcw,f"{mcwn}×"),
        stat_card("Lieblingsphrase",tphr,"häufigstes Wortpaar"),
        stat_card("Top-Emoji",te,f"{ten}×"),
        stat_card("Trend",trend_label,"erste vs. zweite Hälfte"),
        stat_card("Mehrfach-Schreiber",
                  serie_counts.most_common(1)[0][0] if serie_counts else "–",
                  f"{serie_counts.most_common(1)[0][1]} Serien à 3+" if serie_counts else ""),
        stat_card("Sprecherwechsel",f"{sw_idx:.0f}%","wie oft der Sender wechselt"),
    )

    # ── Stats dict for comparison / history ─────────────────
    export_stats = {
        "total":        total,
        "active_days":  act_days,
        "date_range":   date_range,
        "avg_chars":    round(av_chars, 1),
        "emoji_msgs":   emo_msgs,
        "emoji_rate":   round(pct(emo_msgs, total), 1),
        "q_msgs":       q_msgs,
        "streak":       stl,
        "top_word":     mcw,
        "top_emoji":    te,
        "top_phrase":   tphr,
        "trend":        trend_label,
        "act_score":    act_score,
        "word_div":     round(wdiv, 3),
        "mpd":          round(mpd, 1),
        "cons_pct":     round(cons_pct, 1),
        "senders":      {s: c for s, c in sender_counts.most_common()},
    }

    _html = f"""<!doctype html>
<html lang="de">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Signal – {esc(chat_name)}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,400;12..96,600;12..96,800&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
  *,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:'Bricolage Grotesque',sans-serif;background:{BG};color:{TXT};line-height:1.6}}
  .wrap{{max-width:800px;margin:0 auto;padding:0 20px 80px}}
  .header{{padding:48px 0 32px;border-bottom:1px solid {BD};margin-bottom:28px}}
  .eyebrow{{font-family:{MONO};font-size:.68rem;letter-spacing:.16em;text-transform:uppercase;color:{GOLD};margin-bottom:10px}}
  h1{{font-size:clamp(1.8rem,5vw,2.8rem);font-weight:800;color:{WHT};letter-spacing:-.03em;line-height:1.1}}
  .sub{{font-family:{MONO};font-size:.78rem;color:{MUT};margin-top:10px}}
  details summary::-webkit-details-marker{{display:none}}
  details>summary{{outline:none}}
  @media print {{
    body{{background:#fff;color:#111}}
    .wrap{{max-width:100%;padding:0 16px}}
    .header{{padding:24px 0 20px}}
    .eyebrow{{color:#888}}
    h1{{color:#000}}
    .sub{{color:#888}}
    details>summary{{border:1px solid #ddd;border-radius:8px}}
    details[open]>div{{border:1px solid #ddd;border-top:none;border-radius:0 0 8px 8px}}
    /* cards */
    div[style*="background:{{CARD}}"]{{background:#f9f9f9!important;border:1px solid #ddd!important}}
    div[style*="background:#0d0d0d"]{{background:#f5f5f5!important;border:1px solid #ddd!important}}
    /* bars */
    div[style*="background:#161616"]{{background:#eee!important}}
    div[style*="background:#141414"]{{background:#eee!important}}
    /* text colors */
    div[style*="color:{{TXT}}"]{{color:#222!important}}
    div[style*="color:{{MUT}}"]{{color:#666!important}}
    div[style*="color:{{WHT}}"]{{color:#000!important}}
    /* hide nav on print */
    .no-print{{display:none!important}}
  }}
</style>
</head>
<body>
<div class="wrap">
<div class="header">
  <div class="eyebrow">Signal Analyse</div>
  <h1>{esc(chat_name)}</h1>
  <div class="sub">{esc(date_range)} &nbsp;·&nbsp; {nfmt(total)} Nachrichten</div>
</div>
{main_cards}
{section("Aktivitäts-Score",score_svg(act_score,mpd,cons_pct))}
{section("Highlights",hl_cards)}
{section("Aktivitätsverlauf",monthly_chart_svg(day_counts)+divider()+heatmap_svg(day_counts),GRN)}
{section("Nachrichten pro Person",sender_bars)}
{section("Personen im Detail",p_details,BLUE)}
{section("Gesprächsanalyse",sub_label("Wer startet Gespräche?")+init_bars+divider()+sub_label("Ø Antwortzeit")+resp_bars,BLUE)}
{section("Wortvielfalt",wv_html,BLUE)}
{section("Top 20 Wörter",top_words_html,BLUE)}
{section("Top 20 Emojis",top_emo_html)}
{section("Wortentwicklung über Zeit",wdev_html,GOLD)}
{section("Wort-Trends · Letzte 60 Tage",gag_html,GOLD)}
{section("Aktivste Tage (Top 20)","".join(bar_row(k,v,d20_max,GRN) for k,v in day_counts.most_common(20)),GRN)}
{section("Aktivität pro Stunde","".join(bar_row(f"{h:02d}:00",hour_counts[h],hr_max,GRN) for h in range(24)),GRN)}
{section("Aktivität nach Wochentag","".join(bar_row(wd,weekday_counts.get(wd,0),wk_max,GRN) for wd in wd_ord),GRN)}
</div>
</body>
</html>"""
    return _html, export_stats
