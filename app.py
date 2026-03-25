import os, uuid
from flask import Flask, request, jsonify, render_template, Response
from analyzer_core import get_chat_list, run_analysis

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", os.urandom(24))
app.config["MAX_CONTENT_LENGTH"] = 150 * 1024 * 1024  # 150 MB

# Simple in-memory store — fine for hobby use
_store = {}
_reports = {}
MAX_SESSIONS = 30

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
        del _store[next(iter(_store))]

    return jsonify({"session_id": sid, "chats": chats})

@app.route("/analyze", methods=["POST"])
def analyze():
    data    = request.get_json(force=True)
    sid     = data.get("session_id", "")
    chat_id = data.get("chat_id", "")

    if sid not in _store:
        return jsonify({"error": "Session abgelaufen — bitte Datei erneut hochladen."}), 400

    try:
        html = run_analysis(_store[sid], chat_id)
    except Exception as e:
        return jsonify({"error": f"Analyse fehlgeschlagen: {e}"}), 500

    return jsonify({"html": html})

@app.route("/share", methods=["POST"])
def share():
    data    = request.get_json(force=True)
    sid     = data.get("session_id", "")
    chat_id = data.get("chat_id", "")

    if sid not in _store:
        return jsonify({"error": "Session abgelaufen."}), 400

    try:
        html = run_analysis(_store[sid], chat_id)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    share_id = str(uuid.uuid4())[:8]
    _reports[share_id] = html

    # Keep max 100 shared reports
    while len(_reports) > 100:
        del _reports[next(iter(_reports))]

    return jsonify({"share_id": share_id})


@app.route("/r/<share_id>")
def view_report(share_id):
    html = _reports.get(share_id)
    if not html:
        return "<h2 style='font-family:sans-serif;color:#888;text-align:center;margin-top:20%'>Link abgelaufen oder nicht gefunden.</h2>", 404
    return html


@app.errorhandler(413)
def too_large(e):
    return jsonify({"error": "Datei zu groß (max. 150 MB)."}), 413

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)