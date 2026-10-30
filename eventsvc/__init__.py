import json
import time

from flask import Flask, g, jsonify, request
from werkzeug.exceptions import HTTPException

from .db import STATUSES, connect, init_db

MAX_EVENT_ID_LEN = 255
MAX_TYPE_LEN = 100


def validate(data):
    errors = []

    event_id = data.get("event_id")
    if not isinstance(event_id, str) or not event_id.strip():
        errors.append("event_id: обязательная непустая строка")
    elif len(event_id) > MAX_EVENT_ID_LEN:
        errors.append(f"event_id: не длиннее {MAX_EVENT_ID_LEN} символов")

    event_type = data.get("type")
    if not isinstance(event_type, str) or not event_type.strip():
        errors.append("type: обязательная непустая строка")
    elif len(event_type) > MAX_TYPE_LEN:
        errors.append(f"type: не длиннее {MAX_TYPE_LEN} символов")

    if "payload" not in data:
        errors.append("payload: обязательное поле")
    elif not isinstance(data["payload"], dict):
        errors.append("payload: должен быть JSON-объектом")

    return errors


def create_app(db_path="events.db"):
    app = Flask(__name__)
    app.config["DB_PATH"] = db_path
    app.config["MAX_CONTENT_LENGTH"] = 1024 * 1024  # 1 МБ
    init_db(db_path)

    def get_db():
        if "db" not in g:
            g.db = connect(app.config["DB_PATH"])
        return g.db

    @app.teardown_appcontext
    def close_db(_exc):
        db = g.pop("db", None)
        if db is not None:
            db.close()

    @app.errorhandler(HTTPException)
    def http_error(exc):
        return jsonify({"error": exc.description}), exc.code

    @app.post("/webhook")
    def webhook():
        if not request.is_json:
            return jsonify({"error": "Content-Type должен быть application/json"}), 415

        data = request.get_json(silent=True)
        if data is None:
            return jsonify({"error": "Тело запроса не является корректным JSON"}), 400
        if not isinstance(data, dict):
            return jsonify({"error": "Тело запроса должно быть JSON-объектом"}), 400

        errors = validate(data)
        if errors:
            return jsonify({"error": "Некорректное тело запроса", "details": errors}), 422

        db = get_db()
        now = time.time()
      
        db.execute("BEGIN IMMEDIATE")
        try:
            cur = db.execute(
                "INSERT INTO events (event_id, type, payload, received_at) "
                "VALUES (?, ?, ?, ?) ON CONFLICT(event_id) DO NOTHING",
                (data["event_id"], data["type"],
                 json.dumps(data["payload"], ensure_ascii=False), now),
            )
            is_new = cur.rowcount == 1
            if is_new:
                db.execute(
                    "INSERT INTO tasks (event_pk, status, attempts, next_attempt_at, "
                    "created_at, updated_at) VALUES (?, 'pending', 0, ?, ?, ?)",
                    (cur.lastrowid, now, now, now),
                )
            db.execute("COMMIT")
        except BaseException:
            if db.in_transaction:
                db.execute("ROLLBACK")
            raise

        if not is_new:
            return jsonify({"status": "duplicate", "event_id": data["event_id"]}), 200
        return jsonify({"status": "queued", "event_id": data["event_id"]}), 202

    @app.get("/stats")
    def stats():
        db = get_db()
        db.execute("BEGIN")  # оба запроса видят один снимок данных
        try:
            events = {
                r["type"]: r["n"]
                for r in db.execute(
                    "SELECT type, COUNT(*) AS n FROM events GROUP BY type ORDER BY type")
            }
            tasks = dict.fromkeys(STATUSES, 0)
            for r in db.execute("SELECT status, COUNT(*) AS n FROM tasks GROUP BY status"):
                tasks[r["status"]] = r["n"]
        finally:
            db.execute("COMMIT")
        return jsonify({"events_by_type": events, "tasks_by_status": tasks})

    return app
