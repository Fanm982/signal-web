import os, uuid, requests as req_lib
from flask import Flask, request, jsonify, render_template
from analyzer_core import get_chat_list, run_analysis

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", os.urandom(24))
app.config["MAX_CONTENT_LENGTH"] = 150 * 1024 * 1024

# ── Supabase config (set as Railway env vars, see README) ─────
SUPA_URL    = os.environ.get("SUPABASE_URL", "https://zjhufokqykymkbsqwfxl.supabase.co")
SUPA_KEY    = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InpqaHVmb2txeWt5bWtic3F3ZnhsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njg4MzQ1NzQsImV4cCI6MjA4NDQxMDU3NH0.cdAQlSkE1krLBiSdBhrqm-tyE96CRBXQE5qeVgLYdmA")
SUPA_HEADERS = {
    "apikey":        SUPA_KEY,
    "Authorization": f"Bearer {SUPA_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "return=minimal",
}

def _db_save(share_id: str, html: str):
    """Insert report into Supabase."""
    res = req_lib.post(
        f"{SUPA_URL}/rest/v1/signal_reports",
        headers=SUPA_HEADERS,
        json={"share_id": share_id, "html": html},
        timeout=10,
    )
    res.raise_for_status()

def _db_load(share_id: str):
    """Fetch report from Supabase. Returns html string or None."""
    res = req_lib.get(
        f"{SUPA_URL}/rest/v1/signal_reports",
        headers={**SUPA_HEADERS, "Accept": "application/json"},
        params={"share_id": f"eq.{share_id}", "select": "html"},
        timeout=10,
    )
    rows = res.json()
    return rows[0]["html"] if rows else None

# ── In-memory session + html cache ───────────────────────────
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
        return jsonify({"error": "Keine Chats gefunden. Ist das die richtige Datei?"}), 400

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
    cache_key = f"{sid}:{chat_id}"

    html = _html_cache.get(cache_key)

    if not html:
        if sid not in _store:
            return jsonify({"error": "Bitte analysiere den Chat zuerst, dann auf 'Link teilen' klicken."}), 400
        try:
            html = run_analysis(_store[sid], chat_id)
            _html_cache[cache_key] = html
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    share_id = str(uuid.uuid4()).replace("-", "")[:12]

    try:
        _db_save(share_id, html)
    except Exception as e:
        return jsonify({"error": f"Link konnte nicht gespeichert werden: {e}"}), 500

    return jsonify({"share_id": share_id})


@app.route("/r/<share_id>")
def view_report(share_id):
    share_id = "".join(c for c in share_id if c.isalnum())
    try:
        html = _db_load(share_id)
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


@app.errorhandler(413)
def too_large(e):
    return jsonify({"error": "Datei zu groß (max. 150 MB)."}), 413


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)