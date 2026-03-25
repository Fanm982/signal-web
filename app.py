"""
Signal Analyse – Flask Backend
Routes:
  GET  /              → Upload UI
  POST /upload        → Parse JSONL, return chat list
  POST /analyze       → Run analysis, return HTML
  POST /share         → Save report to Supabase, return share_id
  GET  /r/<share_id>  → View shared report
  GET  /compare/<a>/<b> → Side-by-side report comparison
"""
import os, uuid
import requests as req_lib
from flask import Flask, request, jsonify, render_template
from analyzer_core import get_chat_list, run_analysis

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", os.urandom(24))
app.config["MAX_CONTENT_LENGTH"] = 150 * 1024 * 1024  # 150 MB

# ── Supabase ──────────────────────────────────────────────────
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

# ── In-memory session + HTML cache ────────────────────────────
_store      = {}   # sid → file_bytes
_html_cache = {}   # "sid:chat_id" → html
MAX_SESSIONS = 50

# ── Routes ────────────────────────────────────────────────────
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
    # Evict oldest sessions
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

    # Return cached result if available (also used by /share)
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

    # Use cached HTML — avoids re-analysis and doesn't need session alive
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
        return "<html><body style='font-family:sans-serif;background:#0a0a0a;color:#888;display:flex;align-items:center;justify-content:center;height:100vh'><div style='text-align:center'><div style='font-size:2rem;margin-bottom:12px'>🔍</div><div>Ein oder beide Reports nicht gefunden.</div></div></body></html>", 404

    name_a = row_a.get("chat_name") or "Report A"
    name_b = row_b.get("chat_name") or "Report B"
    html_a = row_a["html"].replace('"', "&quot;").replace("</", "&lt;/")
    html_b = row_b["html"].replace('"', "&quot;").replace("</", "&lt;/")

    return f"""<!doctype html>
<html lang="de">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{name_a} vs {name_b}</title>
<link href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,400;12..96,800&family=JetBrains+Mono:wght@400&display=swap" rel="stylesheet">
<style>
  *,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:'Bricolage Grotesque',sans-serif;background:#0a0a0a;color:#ccc;min-height:100vh}}
  .bar{{position:sticky;top:0;z-index:100;background:rgba(10,10,10,.95);
        backdrop-filter:blur(10px);border-bottom:1px solid #1e1e1e;
        padding:0 20px;display:flex;align-items:center;gap:12px;height:52px}}
  .bar-title{{font-weight:800;color:#fff;font-size:.95rem;flex:1;
    overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
  .vs{{font-family:'JetBrains Mono',monospace;font-size:.68rem;
       color:#555;flex-shrink:0}}
  .back{{padding:6px 14px;border:1px solid #1e1e1e;border-radius:8px;
         background:#111;color:#ccc;cursor:pointer;
         font-family:'Bricolage Grotesque',sans-serif;
         font-size:.78rem;font-weight:600;
         text-decoration:none;flex-shrink:0;white-space:nowrap}}
  .split{{display:grid;grid-template-columns:1fr 1fr;
          min-height:calc(100dvh - 52px)}}
  .pane{{border-right:1px solid #1e1e1e;min-width:0}}
  .pane:last-child{{border-right:none}}
  .pane-label{{position:sticky;top:52px;z-index:50;
    background:rgba(10,10,10,.92);backdrop-filter:blur(8px);
    padding:9px 20px;border-bottom:1px solid #1e1e1e;
    font-family:'JetBrains Mono',monospace;font-size:.68rem;
    text-transform:uppercase;letter-spacing:.1em;color:#c9a84c}}
  iframe{{width:100%;border:none;min-height:100vh;display:block}}
  @media(max-width:680px){{
    .split{{grid-template-columns:1fr}}
    .pane{{border-right:none;border-bottom:1px solid #1e1e1e}}
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
    <div class="pane-label">{name_a}</div>
    <iframe srcdoc="{html_a}" loading="lazy" title="{name_a}"></iframe>
  </div>
  <div class="pane">
    <div class="pane-label">{name_b}</div>
    <iframe srcdoc="{html_b}" loading="lazy" title="{name_b}"></iframe>
  </div>
</div>
</body></html>"""


def _not_found_page():
    return (
        "<html><body style='font-family:sans-serif;background:#0a0a0a;"
        "display:flex;align-items:center;justify-content:center;height:100vh;margin:0'>"
        "<div style='text-align:center'>"
        "<div style='font-size:2.5rem;margin-bottom:14px'>📭</div>"
        "<div style='font-size:1rem;color:#666'>Dieser Link existiert nicht oder ist abgelaufen.</div>"
        "</div></body></html>"
    )


@app.errorhandler(413)
def too_large(e):
    return jsonify({"error": "Datei zu groß (max. 150 MB)."}), 413


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)