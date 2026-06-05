"""
analyzer_core.py — Signal JSONL Analysis Engine
Editorial paper-style report generator (German UI).
"""
import json, re, math, html as _html
from collections import Counter, defaultdict
from datetime import datetime, timedelta

# ─────────────────────────────────────────────────────────────
# DESIGN TOKENS — warm editorial palette
# ─────────────────────────────────────────────────────────────
PAPER      = "#f7f3eb"
WHITE      = "#ffffff"
INK        = "#1a1c25"
INK_SOFT   = "#4a4d5b"
MUTE       = "#8a8d9c"
LINE       = "#e6dfcb"
LINE_SOFT  = "#efe8d4"
RUST       = "#c14e30"
RUST_TINT  = "#f1d9d0"
FOREST     = "#2f5044"
AMBER      = "#b87c2c"
INK_BLUE   = "#2a3e5d"

DISPLAY = "'Fraunces',Georgia,'Times New Roman',serif"
BODY    = "'Inter',-apple-system,BlinkMacSystemFont,'Helvetica Neue',sans-serif"
MONO    = "'JetBrains Mono','Fira Mono',monospace"

# ─────────────────────────────────────────────────────────────
# STOPWORDS & FILTERS
# ─────────────────────────────────────────────────────────────
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
    "zwischen","nochmal","eben","gerade","trotzdem","vielleicht","mehr","gut",
    "gerne","sehr","viel","du","gestern","wo","wann","den","des","dem","wirst",
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

# ─────────────────────────────────────────────────────────────
# EMOJI EXTRACTION (grapheme-cluster aware)
# ─────────────────────────────────────────────────────────────
_SKIN = frozenset(range(0x1F3FB, 0x1F400))
_VS16, _ZWJ, _KC = 0xFE0F, 0x200D, 0x20E3
_SKIP = frozenset(list(range(0x1F3FB, 0x1F400)) +
                  [0x200D, 0xFE0F, 0x20E3, 0x2640, 0x2642, 0x2695, 0x2696, 0x2708])

def _is_emo(cp):
    return (0x1F300 <= cp <= 0x1FAFF or 0x2600 <= cp <= 0x27BF or
            0x1F1E0 <= cp <= 0x1F1FF or 0x2300 <= cp <= 0x23FF or
            0x1F900 <= cp <= 0x1F9FF)

def extract_emojis(text):
    result, chars, n, i = [], list(text), len(text), 0
    while i < n:
        cp = ord(chars[i])
        if not _is_emo(cp): i += 1; continue
        if cp in _SKIP:     i += 1; continue
        cluster = chars[i]; i += 1
        while i < n:
            ncp = ord(chars[i])
            if ncp in _SKIN or ncp == _VS16 or ncp == _KC:
                cluster += chars[i]; i += 1
            elif ncp == _ZWJ and i + 1 < n and _is_emo(ord(chars[i+1])):
                cluster += chars[i] + chars[i+1]; i += 2
            else: break
        result.append(cluster)
    return result

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
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
    return (not s or bool(re.fullmatch(r"[A-Za-z0-9+/=]{20,}", s))
            or bool(re.fullmatch(r"\d{6,}", s)))

esc  = lambda s: _html.escape(str(s), quote=True)
avg  = lambda l: sum(l) / len(l) if l else 0.0
pct  = lambda p, t: 100.0 * p / t if t else 0.0
nfmt = lambda n: f"{int(n):,}".replace(",", ".")

def fmt_time(m):
    if m < 1:   return "<1 Min"
    if m < 60:  return f"{m:.0f} Min"
    if m < 1440: return f"{m/60:.1f} Std"
    return f"{m/1440:.1f} Tage"

WDAY = {"Monday":"Montag","Tuesday":"Dienstag","Wednesday":"Mittwoch",
        "Thursday":"Donnerstag","Friday":"Freitag","Saturday":"Samstag","Sunday":"Sonntag"}

def day_de(s): return WDAY[datetime.strptime(s, "%Y-%m-%d").strftime("%A")]

def streak(ds):
    if not ds: return 0, None, None
    bl, bs, be = 1, ds[0], ds[0]; cs, cl = ds[0], 1
    for p, c in zip(ds, ds[1:]):
        if c == p + timedelta(days=1): cl += 1
        else:
            if cl > bl: bl, bs, be = cl, cs, p
            cs, cl = c, 1
    if cl > bl: bl, bs, be = cl, cs, ds[-1]
    return bl, bs, be

def tokenize(text):
    return [w for w in re.findall(r"\b\w+\b", URL_RE.sub(" ", text).lower(), flags=re.UNICODE)
            if len(w) > 2 and w not in GERMAN_STOPWORDS and w not in TECH_WORDS
            and not re.fullmatch(r"\d+", w)]

def initials(name):
    parts = [p for p in re.split(r"\s+", name.strip()) if p]
    if not parts: return "?"
    if len(parts) == 1: return parts[0][:2].upper()
    return (parts[0][0] + parts[-1][0]).upper()

# ─────────────────────────────────────────────────────────────
# PARSE EXPORT
# ─────────────────────────────────────────────────────────────
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
        cid = str(r.get("id", "")).strip()
        if not cid: continue

        if "contact" in r and r["contact"]:
            c = r["contact"]
            profile = (c.get("profileGivenName") or "").strip()
            system  = (c.get("systemGivenName")  or "").strip()
            nick    = (get_nested(c, ("nickname", "given"), "") or "").strip()
            aci     = (c.get("aci")  or "").strip()
            e164    = (c.get("e164") or "").strip()
            pni     = (c.get("pni")  or "").strip()
            best    = nick or system or profile or e164 or cid
            all_ids = [x for x in [aci, e164, pni, cid] if x]
            chats.append({"chat_id": cid, "type": "contact", "display_name": best,
                          "_ids": all_ids, "profile_name": profile,
                          "system_name": system, "nickname": nick})
        elif "group" in r and r["group"]:
            g = r["group"]
            title = (get_nested(g, ("snapshot", "title", "title"), "") or "").strip()
            chats.append({"chat_id": cid, "type": "group",
                          "title": title or f"Gruppe {cid}"})

    # Active chats = those with a "chat" entry
    active_recipient_ids = set()
    chat_id_to_recipient = {}
    for line in lines:
        item = safe_loads(line)
        if not item: continue
        ch = item.get("chat")
        if isinstance(ch, dict):
            cid = str(ch.get("id", "")).strip()
            rid = str(ch.get("recipientId", "")).strip()
            if cid and rid:
                chat_id_to_recipient[cid] = rid
                active_recipient_ids.add(rid)

    # Dedup recipients
    seen = {}
    for c in chats:
        k = c["chat_id"]
        if k not in seen: seen[k] = c
        elif c.get("display_name") and not seen[k].get("display_name"): seen[k] = c

    if active_recipient_ids:
        chats = [c for c in seen.values() if c["chat_id"] in active_recipient_ids]
    else:
        chats = list(seen.values())

    # Number duplicate names
    contact_chats = [c for c in chats if c.get("type") == "contact"]
    name_count = Counter(c["display_name"] for c in contact_chats)
    name_idx = {}
    for c in contact_chats:
        n = c["display_name"]
        if name_count[n] > 1:
            name_idx[n] = name_idx.get(n, 0) + 1
            c["display_name"] = f"{n} {name_idx[n]}"
        for id_val in c.get("_ids", []):
            if id_val: raw_contacts[id_val] = c["display_name"]

    return account_name or "Ich", chats, raw_contacts, chat_id_to_recipient


def get_chat_list(file_bytes):
    _, chats, _, _ = parse_export(file_bytes)
    result = []
    for c in chats:
        if c["type"] == "contact":
            result.append({"chat_id": c["chat_id"], "type": "contact",
                           "name": c.get("display_name", "?")})
        else:
            result.append({"chat_id": c["chat_id"], "type": "group",
                           "name": c.get("title", "?")})
    return sorted(result, key=lambda x: (x["type"], x["name"].lower()))


# ─────────────────────────────────────────────────────────────
# SENDER RESOLVER
# ─────────────────────────────────────────────────────────────
class SenderResolver:
    def __init__(self, target, self_name, raw_contacts):
        self.target = target; self.self_name = self_name
        self.raw = raw_contacts; self._n = 0; self._map = {}

    def resolve(self, entry, sid):
        if "outgoing" in entry and entry["outgoing"] is not None:
            return self.self_name
        if self.target.get("type") != "group":
            return self.target.get("display_name") or "Kontakt"
        if sid:
            if sid in self.raw: return self.raw[sid]
            if sid not in self._map:
                self._n += 1; self._map[sid] = f"Unbekannt {self._n}"
            return self._map[sid]
        return "Unbekannt"


def get_text(entry):
    body = get_nested(entry, ("standardMessage", "text", "body"))
    if isinstance(body, str) and body.strip(): return body.strip()
    body = get_nested(entry, ("chatItem", "standardMessage", "text", "body"))
    if isinstance(body, str) and body.strip(): return body.strip()
    for p in [("text","body"),("message","text","body"),("body",),("content",),("caption",)]:
        v = get_nested(entry, p)
        if isinstance(v, str) and v.strip() and not is_noise(v): return v.strip()
    return ""


def get_ts(item):
    for p in [("dateSent",), ("dateReceived",), ("timestamp",),
              ("standardMessage", "timestamp"),
              ("incoming", "dateReceived"), ("outgoing", "dateReceived")]:
        v = get_nested(item, p)
        if v is None: continue
        try: return int(v)
        except: pass
    return None


def get_sid(item):
    for k in ("authorId", "senderId", "source", "sender"):
        v = item.get(k)
        if isinstance(v, str) and v: return v
    return None


def is_msg(entry):
    if not isinstance(entry, dict): return False
    if "updateMessage" in entry: return False
    return any(k in entry for k in
        ["standardMessage", "incoming", "outgoing", "reaction", "callMessage", "message", "body"])


# ─────────────────────────────────────────────────────────────
# ANALYTICS
# ─────────────────────────────────────────────────────────────
def word_development(sorted_msgs):
    n = len(sorted_msgs)
    if n < 60: return [], []
    third = n // 3
    parts = [sorted_msgs[:third], sorted_msgs[third:2*third], sorted_msgs[2*third:]]

    def freq(msgs):
        c = Counter()
        for m in msgs: c.update(tokenize(m["text"]))
        total = sum(c.values()) or 1
        return {w: v/total for w, v in c.items()}, sum(c.values())

    f1, t1 = freq(parts[0]); f3, t3 = freq(parts[2])
    rising = []; falling = []
    for w in set(f1) | set(f3):
        v1 = f1.get(w, 0); v3 = f3.get(w, 0)
        c1 = round(v1 * t1); c3 = round(v3 * t3)
        if c1 + c3 < 5: continue
        if v1 > 0 and v3 > 0:
            ratio = v3 / v1
            if ratio > 2.5 and c3 >= 4: rising.append((w, ratio, c3))
            if ratio < 0.35 and c1 >= 4: falling.append((w, ratio, c1))
        elif v1 == 0 and c3 >= 6: rising.append((w, 99.0, c3))
        elif v3 == 0 and c1 >= 6: falling.append((w, 0.0, c1))
    return sorted(rising, key=lambda x: -x[1])[:8], sorted(falling, key=lambda x: x[1])[:8]


def word_trends(sorted_msgs):
    if not sorted_msgs: return []
    ts_vals = [m["ts"] for m in sorted_msgs if m["ts"]]
    if not ts_vals: return []
    last_ts = max(ts_vals); cutoff = last_ts - 60*24*60*60*1000
    recent  = [m for m in sorted_msgs if m["ts"] and m["ts"] >= cutoff]
    earlier = [m for m in sorted_msgs if not m["ts"] or m["ts"] < cutoff]
    if len(recent) < 20: return []

    def fc(msgs):
        c = Counter()
        for m in msgs: c.update(tokenize(m["text"]))
        return c, sum(c.values()) or 1

    cr, tr = fc(recent); ce, te = fc(earlier)
    gags = []
    for w, cnt in cr.most_common(200):
        if cnt < 5: break
        base = ce.get(w, 0) / te; cur = cnt / tr
        if base == 0: base = 0.5 / te
        spike = cur / base
        if spike >= 3.0 and cnt >= 5: gags.append((w, spike, cnt))
    return sorted(gags, key=lambda x: -x[1])[:8]


def speaker_change_rate(msgs):
    if len(msgs) < 2: return 0.0
    changes = sum(1 for a, b in zip(msgs, msgs[1:]) if a["sender"] != b["sender"])
    return changes / (len(msgs) - 1) * 100


# ─────────────────────────────────────────────────────────────
# HTML BUILDERS
# ─────────────────────────────────────────────────────────────
def hairline(margin="22px 0"):
    return f'<div style="height:1px;background:{LINE};margin:{margin}"></div>'


def section_header(eb, title, kicker=""):
    kk = (f'<div style="font-family:{BODY};font-size:.9rem;color:{INK_SOFT};'
          f'margin-top:6px;max-width:520px;line-height:1.5">{esc(kicker)}</div>') if kicker else ""
    return (f'<div style="margin:48px 0 24px">'
            f'<div style="font-family:{MONO};font-size:.66rem;letter-spacing:.18em;'
            f'text-transform:uppercase;color:{MUTE};margin-bottom:10px">{esc(eb)}</div>'
            f'<div style="font-family:{DISPLAY};font-size:clamp(1.6rem,4.5vw,2.4rem);'
            f'font-weight:500;color:{INK};letter-spacing:-.02em;line-height:1.1;'
            f'font-variation-settings:&quot;SOFT&quot; 50,&quot;opsz&quot; 144">{esc(title)}</div>'
            f'{kk}</div>')


def stat_card(label, value, sub=""):
    sub_h = (f'<div style="font-family:{MONO};color:{MUTE};font-size:.7rem;'
             f'margin-top:5px;letter-spacing:.04em;line-height:1.5">{esc(sub)}</div>') if sub else ""
    return (f'<div style="background:{WHITE};border:1px solid {LINE};border-radius:14px;'
            f'padding:18px 16px 16px">'
            f'<div style="font-family:{MONO};font-size:.62rem;letter-spacing:.14em;'
            f'text-transform:uppercase;color:{MUTE}">{esc(label)}</div>'
            f'<div style="font-family:{DISPLAY};font-size:1.5rem;font-weight:500;'
            f'color:{INK};margin-top:6px;line-height:1.1;word-break:break-word;'
            f'font-variation-settings:&quot;SOFT&quot; 80,&quot;opsz&quot; 144">{esc(str(value))}</div>'
            f'{sub_h}</div>')


def card_grid(*cards, mincol=158):
    return (f'<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax({mincol}px,1fr));'
            f'gap:10px">' + "".join(cards) + '</div>')


def overview_band(*cells):
    """Connected grid of the headline figures, separated by 1px hairlines."""
    return f'<div class="ov-grid">{"".join(cells)}</div>'


def ov_cell(label, value, sub=""):
    sub_h = f'<div class="ov-sub">{esc(sub)}</div>' if sub else ""
    return (f'<div class="ov-cell">'
            f'<div class="ov-label">{esc(label)}</div>'
            f'<div class="ov-val">{esc(str(value))}</div>'
            f'{sub_h}</div>')


def bar_row(label, value, max_val, accent=RUST, suffix="", label_width=185):
    pw = max(3, int(value / max_val * 100)) if max_val else 3
    return (f'<div style="display:flex;align-items:center;gap:14px;padding:8px 0;'
            f'border-bottom:1px solid {LINE_SOFT}">'
            f'<div style="width:{label_width}px;min-width:{label_width}px;font-size:.9rem;'
            f'color:{INK};overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{esc(label)}</div>'
            f'<div style="flex:1;height:5px;background:{LINE_SOFT};border-radius:3px;overflow:hidden">'
            f'<div style="height:5px;width:{pw}%;background:{accent};border-radius:3px"></div></div>'
            f'<div style="min-width:92px;text-align:right;font-family:{MONO};font-size:.78rem;'
            f'color:{INK_SOFT}">{nfmt(value)}{esc(suffix)}</div></div>')


def emoji_bar_row(emo, value, max_val):
    pw = max(3, int(value / max_val * 100)) if max_val else 3
    return (f'<div style="display:flex;align-items:center;gap:14px;padding:9px 0;'
            f'border-bottom:1px solid {LINE_SOFT}">'
            f'<div style="width:36px;min-width:36px;font-size:1.4rem;line-height:1;'
            f'text-align:center">{emo}</div>'
            f'<div style="flex:1;height:5px;background:{LINE_SOFT};border-radius:3px;overflow:hidden">'
            f'<div style="height:5px;width:{pw}%;background:{RUST};border-radius:3px"></div></div>'
            f'<div style="min-width:50px;text-align:right;font-family:{MONO};font-size:.78rem;'
            f'color:{INK_SOFT}">{nfmt(value)}</div></div>')


def sub_label(text, color=MUTE):
    return (f'<div style="font-family:{MONO};font-size:.66rem;letter-spacing:.14em;'
            f'text-transform:uppercase;color:{color};margin:18px 0 8px;font-weight:500">{esc(text)}</div>')


def pull_quote(text, attribution=""):
    """A large editorial pull-quote."""
    attr = (f'<div style="font-family:{MONO};font-size:.66rem;letter-spacing:.14em;'
            f'text-transform:uppercase;color:{MUTE};margin-top:18px">{esc(attribution)}</div>') if attribution else ""
    return (f'<div style="border-top:1px solid {INK};border-bottom:1px solid {INK};'
            f'padding:32px 0;margin:32px 0">'
            f'<div style="font-family:{DISPLAY};font-size:clamp(1.4rem,3.6vw,2rem);'
            f'font-weight:400;color:{INK};line-height:1.3;letter-spacing:-.01em;font-style:italic;'
            f'font-variation-settings:&quot;SOFT&quot; 30,&quot;opsz&quot; 144">{esc(text)}</div>'
            f'{attr}</div>')


# ─────────────────────────────────────────────────────────────
# SVG VISUALIZATIONS
# ─────────────────────────────────────────────────────────────
def score_ring_svg(score, mpd, cons_pct):
    color = FOREST if score >= 70 else (RUST if score >= 40 else AMBER)
    R = 60; CX = CY = 72; sw = 8
    cf = 2 * math.pi * R; dash = cf * score / 100
    svg = (f'<svg xmlns="http://www.w3.org/2000/svg" width="144" height="144" '
           f'style="flex-shrink:0;display:block">'
           f'<circle cx="{CX}" cy="{CY}" r="{R}" fill="none" stroke="{LINE}" stroke-width="{sw}"/>'
           f'<circle cx="{CX}" cy="{CY}" r="{R}" fill="none" stroke="{color}" stroke-width="{sw}" '
           f'stroke-linecap="round" stroke-dasharray="{dash:.2f} {cf:.2f}" '
           f'transform="rotate(-90 {CX} {CY})"/>'
           f'<text x="{CX}" y="{CY+7}" text-anchor="middle" '
           f'font-family="Fraunces,serif" font-size="36" font-weight="500" fill="{INK}" '
           f'font-variation-settings="&quot;SOFT&quot; 100,&quot;opsz&quot; 144">{score}</text>'
           f'<text x="{CX}" y="{CY+24}" text-anchor="middle" '
           f'font-family="JetBrains Mono,monospace" font-size="9" letter-spacing="1.5" '
           f'fill="{MUTE}">VON 100</text>'
           f'</svg>')
    side = (f'<div style="flex:1;min-width:200px">'
            f'<div style="font-family:{DISPLAY};font-size:1.05rem;color:{INK};'
            f'line-height:1.4;font-style:italic;margin-bottom:14px;'
            f'font-variation-settings:&quot;SOFT&quot; 30,&quot;opsz&quot; 32">'
            f'{_score_description(score)}</div>'
            f'<div style="display:grid;grid-template-columns:1fr 1fr;gap:0;'
            f'border-top:1px solid {LINE};border-bottom:1px solid {LINE}">'
            f'<div style="padding:14px 16px 14px 0;border-right:1px solid {LINE_SOFT}">'
            f'<div style="font-family:{MONO};font-size:.62rem;letter-spacing:.14em;'
            f'text-transform:uppercase;color:{MUTE}">Intensität</div>'
            f'<div style="font-family:{DISPLAY};font-size:1.4rem;color:{INK};margin-top:4px;'
            f'font-variation-settings:&quot;SOFT&quot; 100">{mpd:.1f}</div>'
            f'<div style="font-family:{MONO};font-size:.66rem;color:{MUTE};margin-top:2px">Nachr./Tag</div>'
            f'</div>'
            f'<div style="padding:14px 0 14px 16px">'
            f'<div style="font-family:{MONO};font-size:.62rem;letter-spacing:.14em;'
            f'text-transform:uppercase;color:{MUTE}">Konstanz</div>'
            f'<div style="font-family:{DISPLAY};font-size:1.4rem;color:{INK};margin-top:4px;'
            f'font-variation-settings:&quot;SOFT&quot; 100">{cons_pct:.0f}%</div>'
            f'<div style="font-family:{MONO};font-size:.66rem;color:{MUTE};margin-top:2px">aktive Tage</div>'
            f'</div></div></div>')
    return (f'<div style="display:flex;align-items:center;gap:32px;flex-wrap:wrap">'
            f'{svg}{side}</div>')


def _score_description(score):
    if score >= 80: return "Ein extrem lebhafter Chat — fast täglich, viele Nachrichten."
    if score >= 60: return "Ein aktiver, konstanter Chat."
    if score >= 40: return "Ein solider Austausch, mit Pausen aber regelmäßig."
    if score >= 20: return "Eher sporadisch — kommt und geht."
    return "Ein ruhiger Chat — selten, aber er existiert."


def monthly_chart_svg(day_counts):
    if not day_counts: return ""
    mc = Counter()
    for dk, c in day_counts.items(): mc[dk[:7]] += c
    months = sorted(mc)
    if not months: return ""
    if len(months) > 24: months = months[-24:]
    vals = [mc[m] for m in months]; max_v = max(vals) or 1
    W, H = 680, 180; pl = 36; pb = 30; pt = 16; pr = 14
    cw = W - pl - pr; ch = H - pt - pb; n = len(months)
    bw = max(4, cw / n * 0.66); gap = cw / n

    def ma(vs, w=3):
        out = []
        for i in range(len(vs)):
            s = max(0, i - w + 1); out.append(avg(vs[s:i+1]))
        return out

    mavg = ma(vals, 3); bars = ""; last_yr = None
    for i, (m, v) in enumerate(zip(months, vals)):
        x = pl + i*gap + gap/2 - bw/2; bh = (v/max_v)*ch; y = pt + ch - bh
        bars += (f'<rect x="{x:.1f}" y="{y:.1f}" width="{bw:.1f}" height="{bh:.1f}" '
                 f'rx="2" fill="{RUST}" opacity="0.85"/>')
        yr = m[:4]
        if yr != last_yr or i == 0:
            last_yr = yr; lx = pl + i*gap + gap/2
            bars += (f'<text x="{lx:.1f}" y="{H-6}" font-family="JetBrains Mono,monospace" '
                     f'font-size="9" fill="{MUTE}" text-anchor="middle">{m[5:]}/{m[2:4]}</text>')

    pts = [f"{pl+i*gap+gap/2:.1f},{pt+ch-(v/max_v)*ch:.1f}" for i, v in enumerate(mavg)]
    trend = (f'<polyline points="{" ".join(pts)}" fill="none" stroke="{FOREST}" '
             f'stroke-width="1.5" stroke-linejoin="round" opacity="0.7"/>')

    yl = ""
    for frac in [0.25, 0.5, 0.75, 1.0]:
        val = int(max_v * frac); ly = pt + ch - frac*ch
        yl += (f'<line x1="{pl}" y1="{ly:.1f}" x2="{W-pr}" y2="{ly:.1f}" stroke="{LINE_SOFT}" '
               f'stroke-width="1"/>'
               f'<text x="{pl-4}" y="{ly+3:.1f}" font-family="JetBrains Mono,monospace" '
               f'font-size="9" fill="{MUTE}" text-anchor="end">{val}</text>')
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" '
            f'style="width:100%;height:auto;display:block;overflow:visible">'
            f'{yl}{bars}{trend}'
            f'<line x1="{pl}" y1="{H-pb}" x2="{W-pr}" y2="{H-pb}" stroke="{INK}" stroke-width="1" opacity="0.4"/>'
            f'</svg>')


def heatmap_svg(day_counts):
    if not day_counts: return ""
    dl = sorted(day_counts)
    start = datetime.strptime(dl[0], "%Y-%m-%d").date()
    end   = datetime.strptime(dl[-1], "%Y-%m-%d").date()
    s_mon = start - timedelta(days=start.weekday())
    e_sun = end   + timedelta(days=6 - end.weekday())
    max_c = max(day_counts.values()) or 1

    def color(c):
        if c == 0: return LINE_SOFT
        p = c / max_c
        if p < .20: return "#e9c4ba"
        if p < .45: return "#dba090"
        if p < .75: return "#cf755e"
        return RUST

    CS = 12; G = 2; S = CS + G
    weeks = []; d = s_mon
    while d <= e_sun:
        week = [(d + timedelta(days=i), day_counts.get((d + timedelta(days=i)).strftime("%Y-%m-%d"), 0))
                for i in range(7)]
        weeks.append(week); d += timedelta(weeks=1)

    n = len(weeks); PL = 30; PT = 24; PB = 10
    W = PL + n*S + 6; H = PT + 7*S + PB
    cells = ""; labels = ""; last_m = None
    for wi, week in enumerate(weeks):
        m = week[0][0].month
        if m != last_m:
            last_m = m; lx = PL + wi*S
            labels += (f'<text x="{lx}" y="{PT-7}" font-family="JetBrains Mono,monospace" '
                       f'font-size="9" letter-spacing="1" fill="{MUTE}">{week[0][0].strftime("%b").upper()}</text>')
        for di, (dd, cnt) in enumerate(week):
            x = PL + wi*S; y = PT + di*S
            cells += (f'<rect x="{x}" y="{y}" width="{CS}" height="{CS}" rx="2" fill="{color(cnt)}">'
                      f'<title>{dd.strftime("%d.%m.%Y")}: {cnt}</title></rect>')

    for di, name in enumerate(["Mo", "", "Mi", "", "Fr", "", ""]):
        if not name: continue
        y = PT + di*S + CS//2 + 3
        labels += (f'<text x="{PL-5}" y="{y}" font-family="JetBrains Mono,monospace" '
                   f'font-size="9" letter-spacing="1" fill="{MUTE}" text-anchor="end">{name.upper()}</text>')

    return (f'<div style="overflow-x:auto;padding-bottom:6px;-webkit-overflow-scrolling:touch">'
            f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" '
            f'style="min-width:{W}px;height:{H}px;display:block">'
            f'{labels}{cells}</svg></div>')


def response_profile_bar(resp_times):
    if not resp_times: return ""
    bucket_defs = [
        ("Sofort",    "unter 1 Minute",      FOREST,   lambda t: t < 1),
        ("Schnell",   "1–10 Minuten",        "#6c8b7a", lambda t: t < 10),
        ("Normal",    "10–60 Minuten",       AMBER,    lambda t: t < 60),
        ("Spät",      "1–24 Stunden",        "#d18861", lambda t: t < 1440),
        ("Sehr spät", "mehr als 24 Stunden", RUST,     lambda t: True),
    ]
    buckets = {d[0]: 0 for d in bucket_defs}
    for t in resp_times:
        for label, _, _, check in bucket_defs:
            if check(t): buckets[label] += 1; break
    total = sum(buckets.values()) or 1
    segs = ""; legend_parts = []
    for label, desc, color, _ in bucket_defs:
        cnt = buckets[label]
        if cnt == 0: continue
        p = pct(cnt, total)
        segs += (f'<div style="flex:{p:.1f};background:{color};height:10px;min-width:4px" '
                 f'title="{label} ({desc}): {cnt} ({p:.0f}%)"></div>')
        legend_parts.append(
            f'<span style="display:inline-flex;align-items:center;gap:6px;font-family:{MONO};'
            f'font-size:.7rem;color:{INK_SOFT}">'
            f'<span style="width:8px;height:8px;border-radius:2px;background:{color}"></span>'
            f'{label} <span style="color:{MUTE}">· {desc}</span></span>')
    legend = '<div style="display:flex;flex-wrap:wrap;gap:8px 14px;margin-top:10px">' + "".join(legend_parts) + '</div>'
    return (f'<div style="display:flex;border-radius:3px;overflow:hidden;margin:8px 0 2px">{segs}</div>'
            f'{legend}')


def person_card(name, pd, total_msgs, avg_resp_val, resp_times):
    count = pd["count"]; p = pct(count, total_msgs)
    av_ch = avg(pd["chars"]); av_wo = avg(pd["word_lens"])
    er = pct(pd["emoji_n"], count); qr = pct(pd["q_n"], count)
    top5 = ", ".join(w for w, _ in pd["words"].most_common(5)) or "–"
    resp_str = fmt_time(avg_resp_val) if avg_resp_val else "–"
    init = initials(name)

    mini = (f'<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(110px,1fr));'
            f'gap:0;border-top:1px solid {LINE_SOFT};border-bottom:1px solid {LINE_SOFT};margin:14px 0">'
            + _mini_stat("Nachrichten", nfmt(count), f"{p:.1f}%")
            + _mini_stat("Ø Zeichen", f"{av_ch:.0f}", "pro Nachr.")
            + _mini_stat("Ø Wörter", f"{av_wo:.1f}", "pro Nachr.")
            + _mini_stat("Emoji-Rate", f"{er:.1f}%", "")
            + _mini_stat("Fragen", f"{qr:.1f}%", "mit ?")
            + _mini_stat("Ø Antwort", resp_str, "")
            + '</div>'
            + sub_label("Lieblingswörter")
            + f'<div style="font-size:.9rem;color:{INK};margin-bottom:6px;line-height:1.7">{esc(top5)}</div>'
            + sub_label("Antwort-Profil")
            + response_profile_bar(resp_times))

    avatar = (f'<div style="width:42px;height:42px;border-radius:50%;background:{RUST_TINT};'
              f'color:{RUST};display:flex;align-items:center;justify-content:center;'
              f'font-family:{DISPLAY};font-size:.95rem;font-weight:600;flex-shrink:0;'
              f'font-variation-settings:&quot;SOFT&quot; 60">{esc(init)}</div>')

    return (f'<details style="margin-bottom:10px">'
            f'<summary style="display:flex;align-items:center;gap:14px;padding:14px 18px;'
            f'cursor:pointer;list-style:none;border-radius:14px;background:{WHITE};'
            f'border:1px solid {LINE}">'
            f'{avatar}'
            f'<div style="flex:1;min-width:0">'
            f'<div style="font-family:{DISPLAY};font-weight:500;color:{INK};font-size:1.05rem;'
            f'font-variation-settings:&quot;SOFT&quot; 50">{esc(name)}</div>'
            f'<div style="font-family:{MONO};color:{MUTE};font-size:.7rem;margin-top:2px;'
            f'letter-spacing:.04em">{nfmt(count)} Nachrichten · {p:.1f}%</div>'
            f'</div>'
            f'<div style="color:{MUTE};font-family:{MONO};font-size:.78rem">↓</div>'
            f'</summary>'
            f'<div style="background:{WHITE};border:1px solid {LINE};border-top:none;'
            f'border-radius:0 0 14px 14px;padding:4px 18px 18px;margin-top:-1px">{mini}</div>'
            f'</details>')


def _mini_stat(label, value, sub=""):
    sub_h = (f'<div style="font-family:{MONO};color:{MUTE};font-size:.65rem;margin-top:2px;'
             f'letter-spacing:.04em">{esc(sub)}</div>') if sub else ""
    return (f'<div style="padding:12px 12px 12px 0;border-right:1px solid {LINE_SOFT}">'
            f'<div style="font-family:{MONO};font-size:.6rem;letter-spacing:.14em;'
            f'text-transform:uppercase;color:{MUTE}">{esc(label)}</div>'
            f'<div style="font-family:{DISPLAY};font-size:1.15rem;color:{INK};margin-top:4px;'
            f'font-variation-settings:&quot;SOFT&quot; 100">{esc(value)}</div>'
            f'{sub_h}</div>')


def wdev_col(items, arrow, color):
    if not items: return f"<p style='color:{MUTE};font-size:.85rem;font-style:italic'>Zu wenig Daten.</p>"
    m = items[0][2]; rows = ""
    for w, ratio, cnt in items:
        pw = max(4, int(cnt/m*100)); rat = f"{ratio:.1f}×" if ratio < 90 else "neu"
        rows += (f'<div style="display:flex;align-items:center;gap:10px;padding:6px 0;'
                 f'border-bottom:1px solid {LINE_SOFT}">'
                 f'<div style="width:24px;color:{color};font-family:{MONO};font-size:.85rem">{arrow}</div>'
                 f'<div style="flex:1;font-family:{BODY};font-size:.92rem;color:{INK};'
                 f'overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{esc(w)}</div>'
                 f'<div style="min-width:78px;text-align:right;font-family:{MONO};font-size:.72rem;'
                 f'color:{MUTE}">{rat} · {cnt}×</div>'
                 f'</div>')
    return rows


# ─────────────────────────────────────────────────────────────
# MAIN ANALYSIS
# ─────────────────────────────────────────────────────────────
def run_analysis(file_bytes, chat_id):
    self_name, chats, raw_contacts, chat_id_to_recipient = parse_export(file_bytes)
    target = next((c for c in chats if c["chat_id"] == chat_id), None)
    if not target:
        return _error_page("Chat nicht gefunden.")

    chat_name = (target.get("display_name") if target["type"] == "contact"
                 else target.get("title")) or "Chat"
    is_group = target["type"] == "group"
    resolver  = SenderResolver(target, self_name, raw_contacts)
    lines = file_bytes.decode("utf-8", errors="replace").splitlines()

    messages = []; sender_counts = Counter(); day_counts = Counter()
    hour_counts = Counter({h: 0 for h in range(24)})
    weekday_counts = Counter({"Montag": 0, "Dienstag": 0, "Mittwoch": 0,
                              "Donnerstag": 0, "Freitag": 0, "Samstag": 0, "Sonntag": 0})
    word_counts = Counter(); emoji_counts = Counter(); bigram_counts = Counter()
    q_msgs = emo_msgs = night_msgs = 0
    lengths_chars = []
    person_data = defaultdict(lambda: {"count": 0, "chars": [], "word_lens": [],
                                       "emoji_n": 0, "q_n": 0, "words": Counter()})
    _last_s = None; _cur_b = 0; serie_counts = Counter()

    for idx, line in enumerate(lines):
        item = safe_loads(line)
        if not item: continue
        entry = item.get("chatItem", item)
        if not is_msg(entry): continue
        cid = first_existing(entry,
            [("chatId",), ("recipient", "id"), ("conversationId",), ("threadId",)], None)
        if cid is None: continue
        resolved_cid = chat_id_to_recipient.get(str(cid), str(cid))
        if resolved_cid != chat_id: continue

        text = get_text(entry); ts = get_ts(entry); sid = get_sid(entry)
        sender = resolver.resolve(entry, sid)
        sender_counts[sender] += 1
        pd = person_data[sender]; pd["count"] += 1

        # Same-sender streaks
        if sender != _last_s:
            if _last_s and _cur_b >= 3: serie_counts[_last_s] += 1
            _last_s = sender; _cur_b = 1
        else: _cur_b += 1

        if not text: continue
        when_str = ""
        if ts:
            try: when_str = datetime.fromtimestamp(ts/1000).strftime("%d.%m. %H:%M")
            except: pass

        messages.append({"text": text, "ts": ts, "sender": sender, "order": idx, "when": when_str})
        lengths_chars.append(len(text))
        tokens = tokenize(text)
        pd["chars"].append(len(text)); pd["word_lens"].append(len(tokens))
        pd["words"].update(tokens)
        if _EMO_RE.search(text): pd["emoji_n"] += 1; emo_msgs += 1
        if "?" in text: pd["q_n"] += 1; q_msgs += 1

        if ts:
            try:
                dt = datetime.fromtimestamp(ts/1000); dk = dt.strftime("%Y-%m-%d")
                day_counts[dk] += 1; hour_counts[dt.hour] += 1
                weekday_counts[day_de(dk)] += 1
                if dt.hour >= 22 or dt.hour < 5: night_msgs += 1
            except: pass

        word_counts.update(tokens)
        if len(tokens) >= 2: bigram_counts.update(zip(tokens, tokens[1:]))
        emoji_counts.update(extract_emojis(text))

    if _last_s and _cur_b >= 3: serie_counts[_last_s] += 1

    # ── Stats ─────────────────────────────────────────────────
    total = len(messages); act_days = len(day_counts)
    if total == 0:
        return _error_page("In diesem Chat wurden keine Textnachrichten gefunden.")
    av_chars = avg(lengths_chars)
    dates_s = sorted(datetime.strptime(d, "%Y-%m-%d").date() for d in day_counts)
    total_days = (dates_s[-1] - dates_s[0]).days + 1 if len(dates_s) >= 2 else 1
    mpd = total / act_days if act_days else 0.0
    cons_pct = pct(act_days, total_days)
    streak_d = [datetime.combine(d, datetime.min.time()) for d in dates_s]
    stl, sts, ste = streak(streak_d)
    max_gap = max(((b - a).days for a, b in zip(dates_s, dates_s[1:])), default=0)
    mad = day_counts.most_common(1)[0] if day_counts else ("-", 0)
    mah = hour_counts.most_common(1)[0] if hour_counts else (0, 0)
    maw = weekday_counts.most_common(1)[0] if weekday_counts else ("-", 0)
    quiet_w = min(weekday_counts, key=weekday_counts.get) if weekday_counts else "-"
    mcw = word_counts.most_common(1)[0][0] if word_counts else "–"
    mcwn = word_counts.most_common(1)[0][1] if word_counts else 0
    tbig = bigram_counts.most_common(1)[0][0] if bigram_counts else ("–", "–")
    tphr = " ".join(tbig)
    te = emoji_counts.most_common(1)[0][0] if emoji_counts else "–"
    ten = emoji_counts.most_common(1)[0][1] if emoji_counts else 0
    uw = len(word_counts); twt = sum(word_counts.values())
    wdiv = uw / twt if twt else 0.0
    date_range = (f"{dates_s[0].strftime('%d.%m.%Y')} – {dates_s[-1].strftime('%d.%m.%Y')}"
                  if dates_s else "–")
    streak_sub = (f"{sts.date().strftime('%d.%m.')} → {ste.date().strftime('%d.%m.%Y')}"
                  if sts and ste else "")

    sorted_msgs = sorted(messages, key=lambda m: (m["ts"] if m["ts"] else 10**18, m["order"]))

    # Trend
    trend_label = "→ Stabil"
    if len(dates_s) >= 6:
        month_c = Counter()
        for dk, c in day_counts.items(): month_c[dk[:7]] += c
        ms = sorted(month_c); half = len(ms) // 2
        if half > 0:
            r1 = avg([month_c[m] for m in ms[:half]])
            r2 = avg([month_c[m] for m in ms[half:]])
            if r2 > r1 * 1.2: trend_label = "↑ Wird aktiver"
            elif r2 < r1 * 0.8: trend_label = "↓ Wird ruhiger"

    act_score = int(min(1.0, mpd / 40) * 50 + (cons_pct / 100) * 50)

    # Response times + initiation
    resp_times_per = defaultdict(list); initiator_cnt = Counter()
    prev_ts_r = prev_sender_r = prev_ts_i = None
    for msg in sorted_msgs:
        ts_m = msg["ts"]
        if prev_sender_r and msg["sender"] != prev_sender_r and ts_m and prev_ts_r:
            delta = (ts_m - prev_ts_r) / 60000
            if 0 < delta < 1440: resp_times_per[msg["sender"]].append(delta)
        prev_sender_r = msg["sender"]; prev_ts_r = ts_m
        if ts_m:
            if prev_ts_i is None or (ts_m - prev_ts_i) / 60000 > 180:
                initiator_cnt[msg["sender"]] += 1
            prev_ts_i = ts_m

    avg_resp = {s: avg(ts) for s, ts in resp_times_per.items() if len(ts) >= 8}

    rising, falling = word_development(sorted_msgs)
    gags = word_trends(sorted_msgs)
    sw_idx = speaker_change_rate(sorted_msgs)

    # ── Build HTML ────────────────────────────────────────────
    wd_ord = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"]
    hr_max = max(hour_counts.values()) if hour_counts else 1
    wk_max = max(weekday_counts.values()) if weekday_counts else 1
    d20_max = day_counts.most_common(1)[0][1] if day_counts else 1
    s_max = max(sender_counts.values()) if sender_counts else 1
    i_total = sum(initiator_cnt.values()) or 1
    i_max = max(initiator_cnt.values()) if initiator_cnt else 1
    tw_items = word_counts.most_common(20); tw_max = tw_items[0][1] if tw_items else 1
    te_items = emoji_counts.most_common(20); te_max = te_items[0][1] if te_items else 1

    sender_bars = "".join(bar_row(s, c, s_max, RUST, f"  ({pct(c,total):.1f}%)")
                          for s, c in sender_counts.most_common())
    init_bars = "".join(bar_row(s, c, i_max, FOREST, f"  ({pct(c,i_total):.0f}%)")
                        for s, c in initiator_cnt.most_common())
    r_sorted = sorted(avg_resp.items(), key=lambda x: x[1])
    r_max = max(avg_resp.values()) if avg_resp else 1
    resp_bars = ("".join(bar_row(s, round(t), r_max, AMBER, f"  ({fmt_time(t)})")
                         for s, t in r_sorted)
                 if r_sorted else
                 f"<p style='color:{MUTE};font-size:.88rem;font-style:italic'>"
                 f"Zu wenig Daten (mind. 8 Antworten/Person).</p>")
    p_details = "".join(person_card(name, person_data[name], total,
                                    avg_resp.get(name), resp_times_per.get(name, []))
                        for name, _ in sender_counts.most_common())
    top_words_html = "".join(bar_row(w, c, tw_max, INK_BLUE) for w, c in tw_items)
    top_emo_html = ("".join(emoji_bar_row(e, c, te_max) for e, c in te_items)
                    or f"<p style='color:{MUTE};font-size:.88rem;font-style:italic'>Keine Emojis gefunden.</p>")

    wdev_html = (
        f'<div style="font-family:{MONO};font-size:.7rem;color:{MUTE};margin-bottom:18px;'
        f'letter-spacing:.04em">Vergleich: erste ⅓ vs. letzte ⅓ des Verlaufs</div>'
        f'<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));'
        f'gap:32px">'
        f'<div style="min-width:0">' + sub_label("Aufsteigende Wörter", FOREST) + wdev_col(rising, "↑", FOREST) + '</div>'
        f'<div style="min-width:0">' + sub_label("Absteigende Wörter", RUST) + wdev_col(falling, "↓", RUST) + '</div>'
        f'</div>')

    gag_max = gags[0][2] if gags else 1
    gag_html = ("".join(
        f'<div style="display:flex;align-items:center;gap:12px;padding:7px 0;'
        f'border-bottom:1px solid {LINE_SOFT}">'
        f'<div style="width:170px;font-size:.92rem;color:{INK};overflow:hidden;'
        f'text-overflow:ellipsis;white-space:nowrap">{esc(w)}</div>'
        f'<div style="flex:1;height:5px;background:{LINE_SOFT};border-radius:3px;overflow:hidden">'
        f'<div style="height:5px;width:{max(4,int(cnt/gag_max*100))}%;background:{RUST};'
        f'border-radius:3px"></div></div>'
        f'<div style="min-width:118px;text-align:right;font-family:{MONO};font-size:.74rem;'
        f'color:{MUTE}">{spike:.1f}× · {cnt}×</div></div>'
        for w, spike, cnt in gags)
        or f"<p style='color:{MUTE};font-size:.88rem;font-style:italic'>Mind. 60 Tage Verlauf nötig.</p>")

    wv_html = (
        f'<div style="display:grid;grid-template-columns:auto 1fr;gap:32px;align-items:start">'
        f'<div>'
        f'<div style="font-family:{MONO};font-size:.62rem;letter-spacing:.14em;'
        f'text-transform:uppercase;color:{MUTE}">Score</div>'
        f'<div style="font-family:{DISPLAY};font-size:3.6rem;font-weight:500;color:{INK};'
        f'line-height:1;letter-spacing:-.03em;margin-top:6px;'
        f'font-variation-settings:&quot;SOFT&quot; 100,&quot;opsz&quot; 144">{wdiv:.3f}</div>'
        f'<div style="color:{MUTE};font-family:{MONO};font-size:.72rem;margin-top:8px;line-height:1.6;'
        f'max-width:220px">0 = immer gleiche Wörter<br>1 = jedes Wort einmalig</div>'
        f'</div>'
        f'<div style="display:grid;grid-template-columns:1fr 1fr;gap:0;'
        f'border-top:1px solid {LINE};border-bottom:1px solid {LINE};align-self:center">'
        + _mini_stat("Einzigartig", nfmt(uw), f"von {nfmt(twt)}")
        + _mini_stat("Nur 1× genutzt", nfmt(sum(1 for c in word_counts.values() if c == 1)), "Wörter")
        + '</div></div>')

    # Overview band — the six headline figures (newspaper style)
    overview = overview_band(
        ov_cell("Nachrichten", nfmt(total), f"Ø {mpd:.1f} pro Tag"),
        ov_cell("Aktive Tage", nfmt(act_days), date_range),
        ov_cell("Ø Länge", f"{av_chars:.0f}", "Zeichen pro Nachricht"),
        ov_cell("Emoji-Nachrichten", nfmt(emo_msgs), f"{pct(emo_msgs,total):.1f}%"),
        ov_cell("Fragen", nfmt(q_msgs), f"{pct(q_msgs,total):.1f}%"),
        ov_cell("Längste Serie", f"{stl} Tage", streak_sub or "in Folge"),
    )

    # Highlight cards grid
    hl_cards = card_grid(
        stat_card("Aktivster Tag", mad[0], f"{mad[1]} Nachrichten"),
        stat_card("Aktivste Stunde", f"{mah[0]:02d}:00", f"{mah[1]} Nachrichten"),
        stat_card("Aktivster Wochentag", maw[0], f"{maw[1]} Nachrichten"),
        stat_card("Ruhigster Wochentag", quiet_w, f"{weekday_counts.get(quiet_w,0)} Nachrichten"),
        stat_card("Nacht-Nachrichten", nfmt(night_msgs), f"{pct(night_msgs,total):.1f}% (22–05 Uhr)"),
        stat_card("Größte Pause", f"{max_gap} Tage", "ohne Nachricht"),
        stat_card("Häufigstes Wort", mcw, f"{mcwn}× verwendet"),
        stat_card("Lieblingsphrase", tphr, "häufigstes Wortpaar"),
        stat_card("Top-Emoji", te, f"{ten}× verwendet"),
        stat_card("Trend", trend_label, "erste vs. zweite Hälfte"),
        stat_card("Mehrfach-Schreiber",
                  serie_counts.most_common(1)[0][0] if serie_counts else "–",
                  f"{serie_counts.most_common(1)[0][1]} Serien à 3+" if serie_counts else "keine"),
        stat_card("Sprecherwechsel", f"{sw_idx:.0f}%", "wie oft der Sender wechselt"),
    )

    # Privacy footer
    footer = (
        f'<div style="margin-top:64px;padding-top:32px;border-top:1px solid {LINE};'
        f'text-align:center">'
        f'<div style="font-family:{MONO};font-size:.66rem;letter-spacing:.18em;'
        f'text-transform:uppercase;color:{MUTE};margin-bottom:8px">Signal Analyse</div>'
        f'<div style="font-family:{BODY};font-size:.78rem;color:{MUTE};line-height:1.7">'
        f'Diese Auswertung wurde lokal auf dem Server berechnet · '
        f'Erstellt am {datetime.now().strftime("%d.%m.%Y")}</div>'
        f'</div>')

    sections_html = []

    # Activity score section
    sections_html.append(section_header(
        "I", "Aktivitätsmuster",
        "Wie oft, wie regelmäßig, wie intensiv — das Pulsbild des Chats."))
    sections_html.append(score_ring_svg(act_score, mpd, cons_pct))

    # Highlights
    sections_html.append(section_header("II", "Höhepunkte", "Die markantesten Datenpunkte."))
    sections_html.append(hl_cards)

    # Timeline
    sections_html.append(section_header(
        "III", "Im Verlauf der Zeit",
        "Wann der Chat besonders gelebt hat — und wann er ruhte."))
    sections_html.append(
        f'<div style="background:{WHITE};border:1px solid {LINE};border-radius:14px;padding:20px">'
        + sub_label("Heatmap · Aktivität pro Tag", RUST)
        + heatmap_svg(day_counts)
        + hairline("24px 0")
        + sub_label("Monatlicher Verlauf", RUST)
        + monthly_chart_svg(day_counts)
        + '</div>')

    # People
    sections_html.append(section_header(
        "IV", "Die Stimmen",
        "Wer hat im Chat wie viel gesagt." if is_group else "Wer wie viel geschrieben hat."))
    sections_html.append(
        f'<div style="background:{WHITE};border:1px solid {LINE};border-radius:14px;padding:20px;margin-bottom:14px">'
        + sender_bars + '</div>')
    sections_html.append(p_details)

    # Conversation dynamics
    sections_html.append(section_header(
        "V", "Wer treibt, wer antwortet",
        "Gesprächs­anfänge und Antwortzeiten."))
    sections_html.append(
        f'<div style="background:{WHITE};border:1px solid {LINE};border-radius:14px;padding:20px">'
        + sub_label("Wer beginnt Gespräche", FOREST) + init_bars
        + hairline("20px 0")
        + sub_label("Ø Antwortzeit", AMBER) + resp_bars
        + '</div>')

    # Words
    sections_html.append(section_header(
        "VI", "Die Wörter",
        "Was am häufigsten gesagt wurde, wie reich der Wortschatz ist."))
    sections_html.append(
        f'<div style="background:{WHITE};border:1px solid {LINE};border-radius:14px;padding:24px;margin-bottom:16px">'
        + wv_html + '</div>')
    sections_html.append(
        f'<div style="background:{WHITE};border:1px solid {LINE};border-radius:14px;padding:20px">'
        + sub_label("Top 20 Wörter", INK_BLUE) + top_words_html + '</div>')

    # Word development
    sections_html.append(section_header(
        "VII", "Wortentwicklung",
        "Welche Wörter zugenommen haben — und welche verschwinden."))
    sections_html.append(
        f'<div style="background:{WHITE};border:1px solid {LINE};border-radius:14px;padding:20px;margin-bottom:14px">'
        + wdev_html + '</div>')
    sections_html.append(
        f'<div style="background:{WHITE};border:1px solid {LINE};border-radius:14px;padding:20px">'
        + sub_label("Wort-Trends · letzte 60 Tage", RUST) + gag_html + '</div>')

    # Emojis
    sections_html.append(section_header("VIII", "Die Emojis", "Welche Bilder am häufigsten gewählt wurden."))
    sections_html.append(
        f'<div style="background:{WHITE};border:1px solid {LINE};border-radius:14px;padding:20px">'
        + top_emo_html + '</div>')

    # Time patterns
    sections_html.append(section_header("IX", "Zeit-Muster", "Wann das Telefon geklingelt hat."))
    sections_html.append(
        f'<div style="background:{WHITE};border:1px solid {LINE};border-radius:14px;padding:20px;margin-bottom:14px">'
        + sub_label("Aktivität pro Stunde", FOREST)
        + "".join(bar_row(f"{h:02d}:00", hour_counts[h], hr_max, FOREST) for h in range(24))
        + '</div>')
    sections_html.append(
        f'<div style="background:{WHITE};border:1px solid {LINE};border-radius:14px;padding:20px;margin-bottom:14px">'
        + sub_label("Aktivität nach Wochentag", FOREST)
        + "".join(bar_row(wd, weekday_counts.get(wd, 0), wk_max, FOREST) for wd in wd_ord)
        + '</div>')
    sections_html.append(
        f'<div style="background:{WHITE};border:1px solid {LINE};border-radius:14px;padding:20px">'
        + sub_label("Aktivste Einzeltage · Top 20", FOREST)
        + "".join(bar_row(k, v, d20_max, FOREST) for k, v in day_counts.most_common(20))
        + '</div>')

    _html_doc = f"""<!doctype html>
<html lang="de">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Signal · {esc(chat_name)}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Fraunces:ital,opsz,wght,SOFT@0,9..144,300..700,0..100;1,9..144,300..700,0..100&family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
  *,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
  html,body{{background:{PAPER}}}
  body{{font-family:{BODY};color:{INK};line-height:1.6;
       font-feature-settings:"ss01","cv01";-webkit-font-smoothing:antialiased}}
  .wrap{{max-width:820px;margin:0 auto;padding:0 22px 80px}}
  .cover{{padding:56px 0 24px}}
  .cover-eyebrow{{font-family:{MONO};font-size:.66rem;letter-spacing:.22em;
    text-transform:uppercase;color:{RUST};margin-bottom:18px;font-weight:500}}
  h1{{font-family:{DISPLAY};font-size:clamp(2.4rem,8vw,4.4rem);font-weight:400;color:{INK};
     letter-spacing:-.035em;line-height:1.02;
     font-variation-settings:"SOFT" 50,"opsz" 144;word-break:break-word}}
  .cover-meta{{font-family:{MONO};font-size:.74rem;color:{MUTE};margin-top:22px;
              letter-spacing:.04em}}
  .cover-meta strong{{color:{INK};font-weight:500}}
  .ov-grid{{display:grid;grid-template-columns:repeat(3,1fr);gap:1px;
    background:{LINE};border:1px solid {LINE};border-radius:14px;overflow:hidden;margin-top:4px}}
  .ov-cell{{background:{WHITE};padding:18px 18px 16px;min-width:0}}
  .ov-label{{font-family:{MONO};font-size:.62rem;letter-spacing:.14em;
    text-transform:uppercase;color:{MUTE}}}
  .ov-val{{font-family:{DISPLAY};font-size:1.9rem;font-weight:500;color:{INK};
    margin-top:6px;line-height:1.05;letter-spacing:-.02em;
    font-variation-settings:"SOFT" 100,"opsz" 144;word-break:break-word}}
  .ov-sub{{font-family:{MONO};color:{MUTE};font-size:.7rem;margin-top:5px;
    letter-spacing:.04em;line-height:1.45;word-break:break-word}}
  @media(max-width:560px){{.ov-grid{{grid-template-columns:repeat(2,1fr)}}}}
  details summary::-webkit-details-marker{{display:none}}
  details>summary{{outline:none;-webkit-tap-highlight-color:transparent}}

  @media print {{
    * {{-webkit-print-color-adjust:exact !important;print-color-adjust:exact !important;
        color-adjust:exact !important}}
    @page {{size:A4 portrait;margin:14mm 12mm}}
    html,body{{background:{PAPER} !important;color:{INK} !important;font-size:10.5pt}}
    .wrap{{max-width:100% !important;padding:0 !important}}
    details>summary{{background:{WHITE} !important;border:1px solid {LINE} !important}}
    details[open]{{break-inside:avoid;page-break-inside:avoid}}
    svg{{max-width:100% !important;overflow:visible !important}}
    div[style*="overflow-x"]{{overflow:visible !important}}
    div[style*="display:grid"]{{break-inside:avoid;page-break-inside:avoid}}
    .no-print{{display:none !important}}
  }}
</style>
</head>
<body>
<div class="wrap">

  <div class="cover">
    <div class="cover-eyebrow">Signal · Eine Auswertung</div>
    <h1>{esc(chat_name)}</h1>
    <div class="cover-meta">
      <strong>{nfmt(total)}</strong> Nachrichten &nbsp;·&nbsp;
      <strong>{esc(date_range)}</strong>
    </div>
  </div>

  {overview}

  {pull_quote(f'„{nfmt(total)} Nachrichten in {nfmt(act_days)} aktiven Tagen — '
              f'das macht im Schnitt {mpd:.1f} pro aktivem Tag.“', 'Die Zahlen')}

  {"".join(sections_html)}

  {footer}

</div>
</body>
</html>"""
    return _html_doc


def _error_page(msg):
    return (f"<!doctype html><html lang='de'><head><meta charset='utf-8'><meta name='viewport' "
            f"content='width=device-width,initial-scale=1'><style>body{{font-family:{BODY};"
            f"background:{PAPER};color:{INK};display:flex;align-items:center;"
            f"justify-content:center;min-height:100vh;margin:0;padding:24px}}"
            f".box{{text-align:center;max-width:380px}}.t{{font-family:{DISPLAY};font-size:1.6rem;"
            f"margin-bottom:8px;font-variation-settings:'SOFT' 50}}.m{{color:{MUTE};font-size:.9rem;"
            f"line-height:1.6}}</style></head><body><div class='box'>"
            f"<div class='t'>Hmm.</div><div class='m'>{esc(msg)}</div></div></body></html>")
