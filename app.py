"""
Signal Analyse — Flask Backend
Routes:
  GET  /                        → Upload UI
  POST /upload                  → Parse JSONL, return chat list
  POST /analyze                 → Run analysis, return HTML
  POST /share                   → Save report to Supabase, return share_id
  GET  /r/<share_id>            → View shared report
  GET  /compare/<id_a>/<id_b>   → Side-by-side report comparison
  GET  /manifest.webmanifest    → PWA manifest
  GET  /sw.js                   → Service worker (offline shell)
  GET  /icon.svg                → App icon (SVG)
  GET  /apple-touch-icon.png    → App icon (PNG, generated at import)
"""
import os, uuid, struct, zlib, math
import requests as req_lib
from flask import Flask, request, jsonify, render_template, Response, make_response
from analyzer_core import (
    get_chat_list, run_analysis,
    PAPER, INK, INK_SOFT, MUTE, LINE, LINE_SOFT, RUST,
    DISPLAY, BODY, MONO, WHITE,
)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", os.urandom(24))
app.config["MAX_CONTENT_LENGTH"] = 150 * 1024 * 1024  # 150 MB

# ───────────────────────────────────────────────────────────────
# Supabase
# ───────────────────────────────────────────────────────────────
SUPA_URL = os.environ.get("SUPABASE_URL",
    "https://zjhufokqykymkbsqwfxl.supabase.co")
SUPA_KEY = os.environ.get("SUPABASE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InpqaHVmb2txeWt5bWtic3F3ZnhsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njg4MzQ1NzQsImV4cCI6MjA4NDQxMDU3NH0.cdAQlSkE1krLBiSdBhrqm-tyE96CRBXQE5qeVgLYdmA")

_HDR = {
    "apikey":        SUPA_KEY,
    "Authorization": f"Bearer {SUPA_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "return=representation",
}

def _db_save(share_id: str, html: str, chat_name: str = ""):
    r = req_lib.post(
        f"{SUPA_URL}/rest/v1/signal_reports",
        headers=_HDR,
        json={"share_id": share_id, "html": html, "chat_name": chat_name},
        timeout=15,
    )
    r.raise_for_status()

def _db_load(share_id: str):
    r = req_lib.get(
        f"{SUPA_URL}/rest/v1/signal_reports",
        headers={**_HDR, "Accept": "application/json"},
        params={"share_id": f"eq.{share_id}", "select": "html,chat_name"},
        timeout=10,
    )
    rows = r.json()
    return rows[0] if rows else None

# ───────────────────────────────────────────────────────────────
# In-memory session + HTML cache
# ───────────────────────────────────────────────────────────────
_store      = {}   # sid → file_bytes
_html_cache = {}   # "sid:chat_id" → html
MAX_SESSIONS = 50

# ───────────────────────────────────────────────────────────────
# Routes
# ───────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload():
    if "file" not in request.files:
        return jsonify({"error": "Keine Datei übermittelt."}), 400
    f = request.files["file"]
    if not f.filename:
        return jsonify({"error": "Keine Datei ausgewählt."}), 400
    file_bytes = f.read()
    if not file_bytes:
        return jsonify({"error": "Datei ist leer."}), 400

    try:
        chats = get_chat_list(file_bytes)
    except Exception as e:
        return jsonify({"error": f"Datei konnte nicht gelesen werden: {e}"}), 400
    if not chats:
        return jsonify({"error": "Keine Chats gefunden — ist das die richtige Datei?"}), 400

    sid = str(uuid.uuid4())
    _store[sid] = file_bytes
    while len(_store) > MAX_SESSIONS:
        del _store[next(iter(_store))]

    return jsonify({"session_id": sid, "chats": chats})


@app.route("/analyze", methods=["POST"])
def analyze():
    data      = request.get_json(force=True)
    sid       = data.get("session_id", "")
    chat_id   = data.get("chat_id", "")
    cache_key = f"{sid}:{chat_id}"

    if sid not in _store:
        return jsonify({"error": "Session abgelaufen — bitte Datei erneut hochladen."}), 400

    if cache_key in _html_cache:
        return jsonify({"html": _html_cache[cache_key]})

    try:
        html = run_analysis(_store[sid], chat_id)
    except Exception as e:
        return jsonify({"error": f"Analyse fehlgeschlagen: {e}"}), 500

    _html_cache[cache_key] = html
    if len(_html_cache) > 20:
        del _html_cache[next(iter(_html_cache))]

    return jsonify({"html": html})


@app.route("/share", methods=["POST"])
def share():
    data      = request.get_json(force=True)
    sid       = data.get("session_id", "")
    chat_id   = data.get("chat_id", "")
    chat_name = data.get("chat_name", "")
    cache_key = f"{sid}:{chat_id}"

    html = _html_cache.get(cache_key)
    if not html:
        if sid not in _store:
            return jsonify({"error": "Bitte zuerst analysieren, dann teilen."}), 400
        try:
            html = run_analysis(_store[sid], chat_id)
            _html_cache[cache_key] = html
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    share_id = str(uuid.uuid4()).replace("-", "")[:12]
    try:
        _db_save(share_id, html, chat_name)
    except Exception as e:
        return jsonify({"error": f"Link konnte nicht erstellt werden: {e}"}), 500

    return jsonify({"share_id": share_id})


@app.route("/r/<share_id>")
def view_report(share_id):
    share_id = "".join(c for c in share_id if c.isalnum())
    try:
        row = _db_load(share_id)
        html = row["html"] if row else None
    except Exception:
        html = None

    if not html:
        return _not_found_page(), 404
    return html


@app.route("/compare/<id_a>/<id_b>")
def compare(id_a, id_b):
    id_a = "".join(c for c in id_a if c.isalnum())
    id_b = "".join(c for c in id_b if c.isalnum())
    try:
        row_a = _db_load(id_a)
        row_b = _db_load(id_b)
    except Exception:
        row_a = row_b = None

    if not row_a or not row_b:
        return _not_found_page("Einer der Reports wurde nicht gefunden."), 404

    name_a = row_a.get("chat_name") or "Report A"
    name_b = row_b.get("chat_name") or "Report B"
    html_a = row_a["html"].replace('"', "&quot;").replace("</", "&lt;/")
    html_b = row_b["html"].replace('"', "&quot;").replace("</", "&lt;/")

    return f"""<!doctype html>
<html lang="de">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="{PAPER}">
<title>{name_a} vs {name_b}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Fraunces:ital,opsz,wght,SOFT@0,9..144,400;0,9..144,500;1,9..144,400&family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@400&display=swap" rel="stylesheet">
<style>
  *,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:{BODY};background:{PAPER};color:{INK};min-height:100vh}}
  .bar{{
    position:sticky;top:0;z-index:100;
    background:rgba(247,243,235,.92);
    backdrop-filter:blur(14px);-webkit-backdrop-filter:blur(14px);
    border-bottom:1px solid {LINE};
    padding:14px env(safe-area-inset-right,16px) 14px env(safe-area-inset-left,16px);
    padding-top:calc(env(safe-area-inset-top,0) + 14px);
    display:flex;align-items:center;gap:14px;
  }}
  .bar-title{{
    font-family:{DISPLAY};font-weight:500;color:{INK};font-size:1rem;flex:1;
    overflow:hidden;text-overflow:ellipsis;white-space:nowrap;letter-spacing:-.01em;
    font-variation-settings:"SOFT" 50,"opsz" 24;
  }}
  .vs{{font-family:{MONO};font-size:.68rem;color:{MUTE};letter-spacing:.14em;
       text-transform:uppercase;flex-shrink:0}}
  .back{{
    padding:8px 14px;border:1px solid {LINE};border-radius:9px;
    background:{WHITE};color:{INK_SOFT};
    font-family:{BODY};font-size:.78rem;font-weight:500;
    text-decoration:none;flex-shrink:0;white-space:nowrap;
  }}
  .split{{
    display:grid;grid-template-columns:1fr 1fr;
    min-height:calc(100dvh - 52px);
  }}
  .pane{{border-right:1px solid {LINE};min-width:0;background:{PAPER}}}
  .pane:last-child{{border-right:none}}
  .pane-label{{
    position:sticky;top:65px;z-index:50;
    background:rgba(247,243,235,.92);backdrop-filter:blur(8px);
    padding:10px 18px;border-bottom:1px solid {LINE_SOFT};
    font-family:{MONO};font-size:.66rem;text-transform:uppercase;
    letter-spacing:.16em;color:{RUST};
  }}
  iframe{{width:100%;border:none;min-height:100vh;display:block;background:{PAPER}}}
  @media(max-width:720px){{
    .split{{grid-template-columns:1fr}}
    .pane{{border-right:none;border-bottom:1px solid {LINE}}}
    .bar-title span.b-name{{display:none}}
  }}
</style>
</head>
<body>
<div class="bar">
  <div class="bar-title">
    <span>{name_a}</span>
    <span class="vs">&nbsp;vs&nbsp;</span>
    <span class="b-name">{name_b}</span>
  </div>
  <a href="javascript:history.back()" class="back">← Zurück</a>
</div>
<div class="split">
  <div class="pane">
    <div class="pane-label">A · {name_a}</div>
    <iframe srcdoc="{html_a}" loading="lazy" title="{name_a}"></iframe>
  </div>
  <div class="pane">
    <div class="pane-label">B · {name_b}</div>
    <iframe srcdoc="{html_b}" loading="lazy" title="{name_b}"></iframe>
  </div>
</div>
</body></html>"""


def _not_found_page(msg="Dieser Link existiert nicht oder ist abgelaufen."):
    return (
        f"<!doctype html><html lang='de'><head><meta charset='utf-8'>"
        f"<meta name='viewport' content='width=device-width,initial-scale=1'>"
        f"<style>body{{font-family:{BODY};background:{PAPER};color:{INK};"
        f"display:flex;align-items:center;justify-content:center;"
        f"min-height:100vh;margin:0;padding:24px;text-align:center}}"
        f".box{{max-width:380px}}.t{{font-family:{DISPLAY};font-size:1.8rem;"
        f"margin-bottom:10px;font-variation-settings:'SOFT' 50, 'opsz' 32}}"
        f".m{{color:{MUTE};font-size:.95rem;line-height:1.6}}"
        f".back{{display:inline-block;margin-top:20px;padding:10px 18px;"
        f"border:1px solid {LINE};border-radius:10px;text-decoration:none;"
        f"color:{INK};font-family:{BODY};font-weight:500;font-size:.85rem;"
        f"background:{WHITE}}}</style></head><body>"
        f"<div class='box'><div class='t'>Nicht gefunden.</div>"
        f"<div class='m'>{msg}</div>"
        f"<a class='back' href='/'>Zurück zur Startseite</a></div></body></html>"
    )


# ───────────────────────────────────────────────────────────────
# PWA: manifest, service worker, icons
# ───────────────────────────────────────────────────────────────
@app.route("/manifest.webmanifest")
def manifest():
    return jsonify({
        "name": "Signal Analyse",
        "short_name": "Signal",
        "description": "Persönliche Signal-Chat-Auswertung",
        "lang": "de",
        "dir": "ltr",
        "start_url": "/",
        "scope": "/",
        "display": "standalone",
        "orientation": "portrait",
        "background_color": PAPER,
        "theme_color": PAPER,
        "icons": [
            {"src": "/icon.svg", "sizes": "any", "type": "image/svg+xml", "purpose": "any"},
            {"src": "/icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "any maskable"},
            {"src": "/icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any maskable"},
            {"src": "/apple-touch-icon.png", "sizes": "180x180", "type": "image/png"},
        ],
        "categories": ["productivity", "utilities"],
    })


@app.route("/sw.js")
def service_worker():
    js = f"""// Signal Analyse · simple offline shell
const VERSION = 'sa-v3';
const CORE = ['/'];

self.addEventListener('install', e => {{
  self.skipWaiting();
  e.waitUntil(caches.open(VERSION).then(c => c.addAll(CORE)).catch(()=>{{}}));
}});

self.addEventListener('activate', e => {{
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== VERSION).map(k => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
}});

self.addEventListener('fetch', e => {{
  const url = new URL(e.request.url);
  // Don't cache POSTs, uploads, share endpoints, or report URLs
  if (e.request.method !== 'GET') return;
  if (url.pathname.startsWith('/upload') || url.pathname.startsWith('/analyze')
      || url.pathname.startsWith('/share') || url.pathname.startsWith('/r/')
      || url.pathname.startsWith('/compare/')) return;

  // Network-first for the index, cache-fallback if offline.
  if (url.pathname === '/' || url.pathname === '/index.html') {{
    e.respondWith(
      fetch(e.request).then(res => {{
        const copy = res.clone();
        caches.open(VERSION).then(c => c.put('/', copy));
        return res;
      }}).catch(() => caches.match('/'))
    );
    return;
  }}

  // Cache-first for static assets.
  e.respondWith(
    caches.match(e.request).then(hit => hit || fetch(e.request).then(res => {{
      if (res.ok && (url.pathname.startsWith('/icon') || url.pathname.endsWith('.png')
          || url.pathname.endsWith('.svg') || url.pathname.endsWith('.webmanifest'))) {{
        const copy = res.clone();
        caches.open(VERSION).then(c => c.put(e.request, copy));
      }}
      return res;
    }}).catch(() => undefined))
  );
}});
"""
    resp = make_response(js)
    resp.headers["Content-Type"] = "application/javascript; charset=utf-8"
    resp.headers["Cache-Control"] = "no-cache, must-revalidate"
    resp.headers["Service-Worker-Allowed"] = "/"
    return resp


@app.route("/icon.svg")
def icon_svg():
    # A geometric ink-on-paper mark: a quote-bubble with the corner curl of a folded page.
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512">
  <rect width="512" height="512" rx="112" fill="{PAPER}"/>
  <!-- Bubble -->
  <path d="M120 144 h220 a48 48 0 0 1 48 48 v140 a48 48 0 0 1 -48 48 h-110 l-58 56 v-56 h-52 a48 48 0 0 1 -48 -48 v-140 a48 48 0 0 1 48 -48 z"
        fill="{RUST}"/>
  <!-- Three dots -->
  <circle cx="180" cy="262" r="14" fill="{PAPER}"/>
  <circle cx="232" cy="262" r="14" fill="{PAPER}"/>
  <circle cx="284" cy="262" r="14" fill="{PAPER}"/>
  <!-- Underscore -->
  <rect x="160" y="402" width="192" height="6" rx="3" fill="{INK}" opacity="0.8"/>
</svg>"""
    resp = make_response(svg)
    resp.headers["Content-Type"] = "image/svg+xml; charset=utf-8"
    resp.headers["Cache-Control"] = "public, max-age=86400"
    return resp


# ───────────────────────────────────────────────────────────────
# PNG icon generation (stdlib only — no Pillow)
# ───────────────────────────────────────────────────────────────
def _hex_to_rgb(h):
    h = h.lstrip("#")
    return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))


def _write_png(width, height, raw_rgb):
    """Write a minimal RGB PNG given a flat bytes buffer of len width*height*3."""
    def chunk(typ, data):
        return (struct.pack(">I", len(data)) + typ + data
                + struct.pack(">I", zlib.crc32(typ + data) & 0xFFFFFFFF))
    ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    # PNG requires a filter byte (0 = None) at the start of every scanline.
    row = width * 3
    filtered = bytearray()
    for y in range(height):
        filtered.append(0)
        filtered.extend(raw_rgb[y * row:(y + 1) * row])
    idat = zlib.compress(bytes(filtered), 9)
    return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", ihdr) + chunk(b"IDAT", idat) + chunk(b"IEND", b"")


def _render_icon_png(size=180):
    """Render the app icon: rust bubble on paper, with three dots."""
    bg = _hex_to_rgb(PAPER)   # paper background
    fg = _hex_to_rgb(RUST)    # bubble color
    dot = _hex_to_rgb(PAPER)  # dot fill
    line = _hex_to_rgb(INK)   # underline

    # Bubble geometry (mirrors icon.svg, scaled to size)
    s = size / 512.0
    # Bubble bounding box
    bx1, by1 = 120*s, 144*s
    bx2, by2 = 388*s, 380*s
    br = 48*s
    tail_apex = (240*s, 436*s)  # bottom of tail
    tail_left = (230*s, 380*s)
    tail_right = (288*s, 380*s)
    # Dots
    d_r = 14*s
    dot_y = 262*s
    dot_xs = [180*s, 232*s, 284*s]
    # Underline
    ul_x1, ul_y1 = 160*s, 402*s
    ul_x2, ul_y2 = 352*s, 408*s

    # Background rounded square handled by iOS, but mask corners for Android maskable
    pixels = bytearray(size * size * 3)

    def set_px(x, y, color):
        idx = (y * size + x) * 3
        pixels[idx]   = color[0]
        pixels[idx+1] = color[1]
        pixels[idx+2] = color[2]

    def in_round_rect(x, y, x1, y1, x2, y2, r):
        if not (x1 <= x <= x2 and y1 <= y <= y2): return False
        # corners
        if x < x1 + r and y < y1 + r:
            return (x - (x1+r))**2 + (y - (y1+r))**2 <= r*r
        if x > x2 - r and y < y1 + r:
            return (x - (x2-r))**2 + (y - (y1+r))**2 <= r*r
        if x < x1 + r and y > y2 - r:
            return (x - (x1+r))**2 + (y - (y2-r))**2 <= r*r
        if x > x2 - r and y > y2 - r:
            return (x - (x2-r))**2 + (y - (y2-r))**2 <= r*r
        return True

    def in_triangle(px, py, p1, p2, p3):
        # Sign-based barycentric
        def sign(a, b, c): return (a[0]-c[0])*(b[1]-c[1]) - (b[0]-c[0])*(a[1]-c[1])
        d1 = sign((px,py), p1, p2)
        d2 = sign((px,py), p2, p3)
        d3 = sign((px,py), p3, p1)
        neg = d1 < 0 or d2 < 0 or d3 < 0
        pos = d1 > 0 or d2 > 0 or d3 > 0
        return not (neg and pos)

    for y in range(size):
        for x in range(size):
            color = bg
            # bubble + tail
            if in_round_rect(x, y, bx1, by1, bx2, by2, br):
                color = fg
            elif in_triangle(x, y, tail_left, tail_right, tail_apex):
                color = fg
            # dots (over bubble)
            if color == fg:
                for cx in dot_xs:
                    if (x - cx)**2 + (y - dot_y)**2 <= d_r*d_r:
                        color = dot
                        break
            # underline (rounded ends approximated)
            if ul_x1 <= x <= ul_x2 and ul_y1 <= y <= ul_y2:
                color = line
            set_px(x, y, color)

    return _write_png(size, size, bytes(pixels))


# Cache generated PNGs at import time so the first request is fast.
_PNG_CACHE = {}
def _icon_png_bytes(size):
    if size not in _PNG_CACHE:
        _PNG_CACHE[size] = _render_icon_png(size)
    return _PNG_CACHE[size]


@app.route("/apple-touch-icon.png")
def apple_touch_icon():
    data = _icon_png_bytes(180)
    resp = make_response(data)
    resp.headers["Content-Type"] = "image/png"
    resp.headers["Cache-Control"] = "public, max-age=86400"
    return resp


@app.route("/icon-192.png")
def icon_192():
    data = _icon_png_bytes(192)
    resp = make_response(data)
    resp.headers["Content-Type"] = "image/png"
    resp.headers["Cache-Control"] = "public, max-age=86400"
    return resp


@app.route("/icon-512.png")
def icon_512():
    data = _icon_png_bytes(512)
    resp = make_response(data)
    resp.headers["Content-Type"] = "image/png"
    resp.headers["Cache-Control"] = "public, max-age=86400"
    return resp


@app.route("/favicon.ico")
def favicon():
    # Reuse the 192 PNG; modern browsers accept this.
    data = _icon_png_bytes(64)
    resp = make_response(data)
    resp.headers["Content-Type"] = "image/png"
    resp.headers["Cache-Control"] = "public, max-age=86400"
    return resp


# ───────────────────────────────────────────────────────────────
# Errors
# ───────────────────────────────────────────────────────────────
@app.errorhandler(413)
def too_large(e):
    return jsonify({"error": "Datei zu groß (max. 150 MB)."}), 413


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
