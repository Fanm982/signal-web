import os, uuid, random, string
import requests as req_lib
from flask import Flask, request, jsonify, render_template
from analyzer_core import get_chat_list, run_analysis

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", os.urandom(24))
app.config["MAX_CONTENT_LENGTH"] = 150 * 1024 * 1024

# ── Supabase ──────────────────────────────────────────────────
SUPA_URL = os.environ.get("SUPABASE_URL",
    "https://zjhufokqykymkbsqwfxl.supabase.co")
SUPA_KEY = os.environ.get("SUPABASE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InpqaHVmb2txeWt5bWtic3F3ZnhsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njg4MzQ1NzQsImV4cCI6MjA4NDQxMDU3NH0.cdAQlSkE1krLBiSdBhrqm-tyE96CRBXQE5qeVgLYdmA")
HDR = {
    "apikey":        SUPA_KEY,
    "Authorization": f"Bearer {SUPA_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "return=representation",
}

def _db(method, table, **kwargs):
    url = f"{SUPA_URL}/rest/v1/{table}"
    r = req_lib.request(method, url, headers=HDR, timeout=10, **kwargs)
    r.raise_for_status()
    return r.json() if r.content else None

def _gen_code():
    """8-char human-readable code like A3K-9PX2"""
    chars = string.ascii_uppercase + string.digits
    raw = "".join(random.choices(chars, k=8))
    return raw[:4] + "-" + raw[4:]

# ── In-memory session + html cache ───────────────────────────
_store      = {}
_html_cache = {}
MAX_SESSIONS = 50

# ── Routes ────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload():
    if "file" not in request.files:
        return jsonify({"error": "Keine Datei."}), 400
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
        return jsonify({"error": "Keine Chats gefunden."}), 400

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
        return jsonify({"error": "Session abgelaufen — Datei erneut hochladen."}), 400

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
    user_code = data.get("user_code") or None
    chat_name = data.get("chat_name", "")
    cache_key = f"{sid}:{chat_id}"

    html = _html_cache.get(cache_key)
    if not html:
        if sid not in _store:
            return jsonify({"error": "Analysiere den Chat zuerst, dann teilen."}), 400
        try:
            html = run_analysis(_store[sid], chat_id)
            _html_cache[cache_key] = html
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    share_id = str(uuid.uuid4()).replace("-", "")[:12]

    try:
        _db("POST", "signal_reports", json={
            "share_id":  share_id,
            "html":      html,
            "user_code": user_code,
            "chat_name": chat_name,
        })
    except Exception as e:
        return jsonify({"error": f"Speichern fehlgeschlagen: {e}"}), 500

    return jsonify({"share_id": share_id})


@app.route("/r/<share_id>")
def view_report(share_id):
    share_id = "".join(c for c in share_id if c.isalnum())
    try:
        rows = _db("GET", "signal_reports",
                   params={"share_id": f"eq.{share_id}", "select": "html"})
        html = rows[0]["html"] if rows else None
    except Exception:
        html = None

    if not html:
        return (
            "<html><body style='font-family:sans-serif;background:#0a0a0a;color:#555;"
            "display:flex;align-items:center;justify-content:center;height:100vh;margin:0'>"
            "<div style='text-align:center'>"
            "<div style='font-size:2.5rem;margin-bottom:16px'>📭</div>"
            "<div style='font-size:1.1rem;color:#888'>Dieser Link existiert nicht oder ist abgelaufen.</div>"
            "</div></body></html>"
        ), 404
    return html


# ── User code / history ───────────────────────────────────────
@app.route("/code/new", methods=["POST"])
def new_code():
    """Generate a new personal code and store it."""
    code = _gen_code()
    try:
        _db("POST", "user_codes", json={"code": code})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"code": code})


@app.route("/code/validate", methods=["POST"])
def validate_code():
    """Check if a code exists."""
    code = (request.get_json(force=True) or {}).get("code", "").strip().upper()
    try:
        rows = _db("GET", "user_codes",
                   params={"code": f"eq.{code}", "select": "code"})
        return jsonify({"valid": bool(rows)})
    except Exception:
        return jsonify({"valid": False})


@app.route("/history", methods=["POST"])
def history():
    """Get all reports for a user code."""
    code = (request.get_json(force=True) or {}).get("code", "").strip().upper()
    if not code:
        return jsonify({"error": "Kein Code."}), 400
    try:
        rows = _db("GET", "signal_reports",
                   params={
                       "user_code": f"eq.{code}",
                       "select":    "share_id,chat_name,chat_type,created_at",
                       "order":     "created_at.desc",
                       "limit":     "20",
                   })
        return jsonify({"reports": rows or []})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Compare ──────────────────────────────────────────────────
@app.route("/compare/<id_a>/<id_b>")
def compare(id_a, id_b):
    """Show two reports side by side."""
    id_a = "".join(c for c in id_a if c.isalnum())
    id_b = "".join(c for c in id_b if c.isalnum())
    try:
        ra = _db("GET", "signal_reports",
                 params={"share_id": f"eq.{id_a}", "select": "html,chat_name"})
        rb = _db("GET", "signal_reports",
                 params={"share_id": f"eq.{id_b}", "select": "html,chat_name"})
    except Exception:
        ra = rb = []

    if not ra or not rb:
        return "<h2 style='font-family:sans-serif;color:#888;text-align:center;margin-top:20%'>Ein oder beide Reports nicht gefunden.</h2>", 404

    name_a = ra[0].get("chat_name") or "Report A"
    name_b = rb[0].get("chat_name") or "Report B"

    # Inline both reports into a split-screen view
    compare_html = f"""<!doctype html>
<html lang="de">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Vergleich: {name_a} vs {name_b}</title>
<link href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,400;12..96,800&family=JetBrains+Mono:wght@400&display=swap" rel="stylesheet">
<style>
  *,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:'Bricolage Grotesque',sans-serif;background:#0a0a0a;color:#ccc}}
  .top{{position:sticky;top:0;z-index:100;background:rgba(10,10,10,.95);
        backdrop-filter:blur(10px);border-bottom:1px solid #1e1e1e;
        padding:14px 20px;display:flex;align-items:center;gap:16px}}
  .top-title{{font-weight:800;color:#fff;font-size:1rem;flex:1}}
  .vs{{font-family:'JetBrains Mono',monospace;font-size:.7rem;color:#555;padding:0 4px}}
  .back{{padding:7px 14px;border:1px solid #1e1e1e;border-radius:8px;
         background:#111;color:#ccc;cursor:pointer;font-family:'Bricolage Grotesque',sans-serif;
         font-size:.78rem;font-weight:600;text-decoration:none}}
  .split{{display:grid;grid-template-columns:1fr 1fr;min-height:calc(100vh - 52px)}}
  .pane{{border-right:1px solid #1e1e1e;overflow:hidden}}
  .pane:last-child{{border-right:none}}
  .pane-label{{
    position:sticky;top:52px;z-index:50;
    background:rgba(10,10,10,.9);backdrop-filter:blur(8px);
    padding:10px 20px;border-bottom:1px solid #1e1e1e;
    font-family:'JetBrains Mono',monospace;font-size:.7rem;
    text-transform:uppercase;letter-spacing:.1em;color:#c9a84c;
  }}
  iframe{{width:100%;border:none;min-height:100vh;display:block}}
  @media(max-width:700px){{
    .split{{grid-template-columns:1fr}}
    .pane{{border-right:none;border-bottom:1px solid #1e1e1e}}
  }}
</style>
</head>
<body>
<div class="top">
  <div class="top-title">
    <span style="color:#ccc">{name_a}</span>
    <span class="vs">vs</span>
    <span style="color:#ccc">{name_b}</span>
  </div>
  <a href="javascript:history.back()" class="back">← Zurück</a>
</div>
<div class="split">
  <div class="pane">
    <div class="pane-label">{name_a}</div>
    <iframe srcdoc="{ra[0]['html'].replace(chr(34), '&quot;').replace(chr(60)+'/', '&lt;/')}" loading="lazy"></iframe>
  </div>
  <div class="pane">
    <div class="pane-label">{name_b}</div>
    <iframe srcdoc="{rb[0]['html'].replace(chr(34), '&quot;').replace(chr(60)+'/', '&lt;/')}" loading="lazy"></iframe>
  </div>
</div>
</body></html>"""
    return compare_html


@app.errorhandler(413)
def too_large(e):
    return jsonify({"error": "Datei zu groß (max. 150 MB)."}), 413


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)