import os, uuid, json
import requests as req_lib
from flask import Flask, request, jsonify, render_template, Response
from analyzer_core import get_chat_list, run_analysis

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", os.urandom(24))
app.config["MAX_CONTENT_LENGTH"] = 150 * 1024 * 1024

# ── Supabase ──────────────────────────────────────────────────
SUPA_URL = os.environ.get("SUPABASE_URL",
    "https://zjhufokqykymkbsqwfxl.supabase.co")
SUPA_KEY = os.environ.get("SUPABASE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InpqaHVmb2txeWt5bWtic3F3ZnhsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njg4MzQ1NzQsImV4cCI6MjA4NDQxMDU3NH0.cdAQlSkE1krLBiSdBhrqm-tyE96CRBXQE5qeVgLYdmA")
_H = lambda extra=None: {
    "apikey":        SUPA_KEY,
    "Authorization": f"Bearer {SUPA_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "return=minimal",
    **(extra or {}),
}

def db_save(share_id, html, stats, user_code=None, chat_name=None):
    res = req_lib.post(
        f"{SUPA_URL}/rest/v1/signal_reports",
        headers=_H(),
        json={"share_id": share_id, "html": html,
              "stats": stats, "user_code": user_code,
              "chat_name": chat_name},
        timeout=15,
    )
    res.raise_for_status()

def db_load_html(share_id):
    share_id = "".join(c for c in share_id if c.isalnum())
    res = req_lib.get(
        f"{SUPA_URL}/rest/v1/signal_reports",
        headers=_H({"Accept": "application/json"}),
        params={"share_id": f"eq.{share_id}", "select": "html"},
        timeout=10,
    )
    rows = res.json()
    return rows[0]["html"] if rows else None

def db_load_stats(share_id):
    share_id = "".join(c for c in share_id if c.isalnum())
    res = req_lib.get(
        f"{SUPA_URL}/rest/v1/signal_reports",
        headers=_H({"Accept": "application/json"}),
        params={"share_id": f"eq.{share_id}", "select": "stats,chat_name,share_id,created_at"},
        timeout=10,
    )
    rows = res.json()
    return rows[0] if rows else None

def db_user_history(user_code):
    res = req_lib.get(
        f"{SUPA_URL}/rest/v1/signal_reports",
        headers=_H({"Accept": "application/json"}),
        params={
            "user_code": f"eq.{user_code}",
            "select":    "share_id,chat_name,stats,created_at",
            "order":     "created_at.desc",
            "limit":     "20",
        },
        timeout=10,
    )
    return res.json() if res.ok else []

# ── In-memory caches ──────────────────────────────────────────
_store      = {}   # sid → file_bytes
_cache      = {}   # "sid:chat_id" → (html, stats)
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
        return jsonify({"error": "Session abgelaufen — bitte Datei erneut hochladen."}), 400
    if cache_key in _cache:
        html, _ = _cache[cache_key]
        return jsonify({"html": html})
    try:
        html, stats = run_analysis(_store[sid], chat_id)
    except Exception as e:
        return jsonify({"error": f"Analyse fehlgeschlagen: {e}"}), 500

    _cache[cache_key] = (html, stats)
    if len(_cache) > 20:
        del _cache[next(iter(_cache))]
    return jsonify({"html": html})


@app.route("/share", methods=["POST"])
def share():
    data      = request.get_json(force=True)
    sid       = data.get("session_id", "")
    chat_id   = data.get("chat_id", "")
    user_code = data.get("user_code") or None
    chat_name = data.get("chat_name") or None
    cache_key = f"{sid}:{chat_id}"

    if cache_key in _cache:
        html, stats = _cache[cache_key]
    elif sid in _store:
        try:
            html, stats = run_analysis(_store[sid], chat_id)
            _cache[cache_key] = (html, stats)
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    else:
        return jsonify({"error": "Bitte analysiere den Chat zuerst, dann teilen."}), 400

    share_id = str(uuid.uuid4()).replace("-", "")[:12]
    try:
        db_save(share_id, html, stats, user_code, chat_name)
    except Exception as e:
        return jsonify({"error": f"Speichern fehlgeschlagen: {e}"}), 500

    return jsonify({"share_id": share_id})


@app.route("/r/<share_id>")
def view_report(share_id):
    html = db_load_html(share_id)
    if not html:
        return (
            "<html><body style='font-family:sans-serif;background:#0a0a0a;"
            "display:flex;align-items:center;justify-content:center;height:100vh;margin:0'>"
            "<div style='text-align:center;color:#555'>"
            "<div style='font-size:2.5rem;margin-bottom:16px'>📭</div>"
            "<div>Dieser Link existiert nicht oder ist abgelaufen.</div>"
            "</div></body></html>"
        ), 404
    return html


@app.route("/history/<user_code>")
def history(user_code):
    user_code = "".join(c for c in user_code if c.isalnum() or c in "-_")
    try:
        rows = db_user_history(user_code)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"reports": rows})


@app.route("/compare")
def compare_page():
    id1 = request.args.get("a", "")
    id2 = request.args.get("b", "")
    if not id1 or not id2:
        return "Zwei Report-IDs benötigt (?a=...&b=...)", 400

    try:
        r1 = db_load_stats(id1)
        r2 = db_load_stats(id2)
    except Exception as e:
        return f"Fehler: {e}", 500

    if not r1 or not r2:
        return "Einer der Reports wurde nicht gefunden.", 404

    return render_template("compare.html", r1=r1, r2=r2)


@app.errorhandler(413)
def too_large(e):
    return jsonify({"error": "Datei zu groß (max. 150 MB)."}), 413


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)