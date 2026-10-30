"""Очередь на таблице tasks.

Статусов всего четыре (pending/success/failed/skipped), поэтому состояния
"взято в работу" нет. Вместо него - аренда: при взятии задачи воркер сдвигает
next_attempt_at на LEASE секунд вперёд. Задача остаётся pending, но другие
воркеры её не видят. Если воркер умер, аренда истекает и задача берётся снова.

Гарантия "не зависнет в pending навсегда": счётчик attempts растёт при КАЖДОМ
взятии (а не после ошибки), поэтому даже если воркер умирает на каждой попытке,
после MAX_ATTEMPTS взятий задача переводится в failed.
"""
import json
import logging
import threading
import time

from .db import connect

log = logging.getLogger(__name__)

MAX_ATTEMPTS = 3
BACKOFF_BASE = 1.0   # паузы между попытками: 1с, 2с (base * 2**(attempt-1))
LEASE_SECONDS = 30.0  # сколько воркер "владеет" задачей, пока обрабатывает


def claim_next(conn, now, max_attempts=MAX_ATTEMPTS, lease=LEASE_SECONDS):
    """Атомарно берёт готовую задачу. Возвращает Row или None."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        while True:
            row = conn.execute(
                "SELECT t.id, t.attempts, e.type, e.payload "
                "FROM tasks t JOIN events e ON e.id = t.event_pk "
                "WHERE t.status = 'pending' AND t.next_attempt_at <= ? "
                "ORDER BY t.next_attempt_at, t.id LIMIT 1",
                (now,),
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return None
            if row["attempts"] >= max_attempts:

                conn.execute(
                    "UPDATE tasks SET status='failed', updated_at=?, "
                    "last_error=COALESCE(last_error || '; ', '') || "
                    "'воркер не вернул результат последней попытки' WHERE id=?",
                    (now, row["id"]),
                )
                continue
            conn.execute(
                "UPDATE tasks SET attempts = attempts + 1, next_attempt_at = ?, "
                "updated_at = ? WHERE id = ?",
                (now + lease, now, row["id"]),
            )
            conn.execute("COMMIT")
            return {
                "id": row["id"],
                "attempt": row["attempts"] + 1,
                "type": row["type"],
                "payload": row["payload"],
            }
    except BaseException:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise


def _finish(conn, task, status, now, error=None):

    conn.execute(
        "UPDATE tasks SET status=?, last_error=?, updated_at=? "
        "WHERE id=? AND status='pending' AND attempts=?",
        (status, error, now, task["id"], task["attempt"]),
    )


def _retry_later(conn, task, now, delay, error):
    conn.execute(
        "UPDATE tasks SET next_attempt_at=?, last_error=?, updated_at=? "
        "WHERE id=? AND status='pending' AND attempts=?",
        (now + delay, error, now, task["id"], task["attempt"]),
    )


def process_one(conn, handlers, now=None, max_attempts=MAX_ATTEMPTS,
                lease=LEASE_SECONDS, backoff_base=BACKOFF_BASE):
    """Берёт одну готовую задачу и обрабатывает. True, если задача была."""
    now = time.time() if now is None else now
    task = claim_next(conn, now, max_attempts, lease)
    if task is None:
        return False

    handler = handlers.get(task["type"])
    if handler is None:
        _finish(conn, task, "skipped", now, f"нет обработчика для типа {task['type']!r}")
        return True

    try:
        handler(json.loads(task["payload"]))
    except Exception as exc: 
        error = f"{type(exc).__name__}: {exc}"
        if task["attempt"] >= max_attempts:
            _finish(conn, task, "failed", now, error)
            log.warning("task %s failed окончательно: %s", task["id"], error)
        else:
            delay = backoff_base * 2 ** (task["attempt"] - 1)
            _retry_later(conn, task, now, delay, error)
            log.info("task %s попытка %s не удалась, повтор через %.1fс: %s",
                     task["id"], task["attempt"], delay, error)
    else:
        _finish(conn, task, "success", now)
    return True


class Worker(threading.Thread):
    def __init__(self, db_path, handlers, poll_interval=0.2):
        super().__init__(daemon=True, name="task-worker")
        self.db_path = db_path
        self.handlers = handlers
        self.poll_interval = poll_interval
        self.stop_event = threading.Event()

    def run(self):
        conn = connect(self.db_path)  # соединение создаётся в своём потоке
        try:
            while not self.stop_event.is_set():
                try:
                    did_work = process_one(conn, self.handlers)
                except Exception:  # поток не должен умирать из-за сбоя БД
                    log.exception("ошибка цикла воркера")
                    did_work = False
                if not did_work:
                    self.stop_event.wait(self.poll_interval)
        finally:
            conn.close()

    def stop(self):
        self.stop_event.set()
