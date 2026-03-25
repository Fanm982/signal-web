import os, uuid, pathlib
from flask import Flask, request, jsonify, render_template, Response
from analyzer_core import get_chat_list, run_analysis

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", os.urandom(24))
app.config["MAX_CONTENT_LENGTH"] = 150 * 1024 * 1024  # 150 MB

# ── In-memory stores ──────────────────────────────────────────
_store        = {}   # sid → file_bytes
_html_cache   = {}   # sid:chat_id → report html  (avoid re-analysis)
MAX_SESSIONS  = 50

# ── Persistent share storage (survives restart) ───────────────
SHARE_DIR = pathlib.Path("/tmp/signal_shares")
SHARE_DIR.mkdir(parents=True, exist_ok=True)

def _save_share(share_id: str, html: str):
    (SHARE_DIR / f"{share_id}.html").write_text(html, encoding="utf-8")

def _load_share(share_id: str):
    p = SHARE_DIR / f"{share_id}.html"
    return p.read_text(encoding="utf-8") if p.exists() else None

def _evict_old_shares(keep=200):
    files = sorted(SHARE_DIR.glob("*.html"), key=lambda f: f.stat().st_mtime)
    for f in files[:-keep]:
        f.unlink(missing_ok=True)

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

    # Evict oldest sessions
    while len(_store) > MAX_SESSIONS:
        oldest = next(iter(_store))
        del _store[oldest]

    return jsonify({"session_id": sid, "chats": chats})


@app.route("/analyze", methods=["POST"])
def analyze():
    data    = request.get_json(force=True)
    sid     = data.get("session_id", "")
    chat_id = data.get("chat_id", "")
    cache_key = f"{sid}:{chat_id}"

    if sid not in _store:
        return jsonify({"error": "Session abgelaufen — bitte Datei erneut hochladen."}), 400

    # Return cached result if available
    if cache_key in _html_cache:
        return jsonify({"html": _html_cache[cache_key]})

    try:
        html = run_analysis(_store[sid], chat_id)
    except Exception as e:
        return jsonify({"error": f"Analyse fehlgeschlagen: {e}"}), 500

    # Cache the result so /share can use it without re-analysis
    _html_cache[cache_key] = html

    # Evict old cache entries (keep last 20)
    if len(_html_cache) > 20:
        del _html_cache[next(iter(_html_cache))]

    return jsonify({"html": html})


@app.route("/share", methods=["POST"])
def share():
    data      = request.get_json(force=True)
    sid       = data.get("session_id", "")
    chat_id   = data.get("chat_id", "")
    cache_key = f"{sid}:{chat_id}"

    # Try cache first — avoids needing the session to still be alive
    html = _html_cache.get(cache_key)

    # If not in cache, try re-analysis (session must still be alive)
    if not html:
        if sid not in _store:
            return jsonify({"error": "Analyse nicht mehr im Cache. Bitte analysiere den Chat nochmal und dann teile den Link."}), 400
        try:
            html = run_analysis(_store[sid], chat_id)
            _html_cache[cache_key] = html
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    share_id = str(uuid.uuid4())[:10]
    _save_share(share_id, html)
    _evict_old_shares(keep=200)

    return jsonify({"share_id": share_id})


@app.route("/r/<share_id>")
def view_report(share_id):
    # Sanitize share_id to prevent path traversal
    share_id = share_id.replace("/", "").replace(".", "").replace("\\", "")
    html = _load_share(share_id)
    if not html:
        return (
            "<html><body style='font-family:sans-serif;background:#0a0a0a;color:#555;"
            "display:flex;align-items:center;justify-content:center;height:100vh;margin:0'>"
            "<div style='text-align:center'>"
            "<div style='font-size:2rem;margin-bottom:12px'>📭</div>"
            "<div style='font-size:1.1rem;color:#888'>Dieser Link ist abgelaufen oder existiert nicht.</div>"
            "</div></body></html>"
        ), 404
    return html


@app.errorhandler(413)
def too_large(e):
    return jsonify({"error": "Datei zu groß (max. 150 MB)."}), 413


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)