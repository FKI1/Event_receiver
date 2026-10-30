import time

import pytest

from eventsvc.worker import claim_next, process_one

T0 = time.time() + 3600


def send(client, event_id="abc-123", type_="payment_succeeded", payload=None):
    return client.post("/webhook", json={
        "event_id": event_id, "type": type_,
        "payload": {"amount": 100} if payload is None else payload,
    })


def task(db):
    return db.execute("SELECT * FROM tasks").fetchone()


def count(db, table):
    return db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


# ---- вспомогательные обработчики -------------------------------------------

def ok(payload):
    pass


def always_fails(payload):
    raise RuntimeError("boom")


class FailsOnce:
    def __init__(self):
        self.calls = 0

    def __call__(self, payload):
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("temporary")


# ---- webhook ---------------------------------------------------------------

def test_webhook_saves_event_and_creates_pending_task(client, db):
    resp = send(client)
    assert resp.status_code == 202
    assert count(db, "events") == 1
    t = task(db)
    assert t["status"] == "pending" and t["attempts"] == 0


def test_duplicate_event_id_does_not_create_second_record(client, db):
    assert send(client).status_code == 202
    resp = send(client)
    assert resp.status_code == 200
    assert resp.get_json()["status"] == "duplicate"
    assert count(db, "events") == 1
    assert count(db, "tasks") == 1


@pytest.mark.parametrize("kwargs, expected", [
    ({"data": "{not json", "content_type": "application/json"}, 400),
    ({"json": [1, 2, 3]}, 400),
    ({"json": {"type": "x", "payload": {}}}, 422),                       # нет event_id
    ({"json": {"event_id": "", "type": "x", "payload": {}}}, 422),       # пустой event_id
    ({"json": {"event_id": "a", "type": 5, "payload": {}}}, 422),        # type не строка
    ({"json": {"event_id": "a", "type": "x", "payload": "str"}}, 422),   # payload не объект
    ({"data": "event_id=a", "content_type": "text/plain"}, 415),
])
def test_invalid_body_returns_4xx_and_stores_nothing(client, db, kwargs, expected):
    resp = client.post("/webhook", **kwargs)
    assert resp.status_code == expected
    assert "error" in resp.get_json()
    assert count(db, "events") == 0 and count(db, "tasks") == 0


# ---- очередь ---------------------------------------------------------------

def test_successful_processing(client, db):
    send(client)
    assert process_one(db, {"payment_succeeded": ok}, now=T0 + 100) is True
    assert task(db)["status"] == "success"
    assert process_one(db, {"payment_succeeded": ok}, now=T0 + 101) is False  # очередь пуста


def test_retries_with_growing_backoff_then_failed(client, db):
    send(client)
    h = {"payment_succeeded": always_fails}
    kw = dict(backoff_base=1.0, lease=30)

    assert process_one(db, h, now=T0 + 100, **kw)           # попытка 1, пауза 1с
    t = task(db)
    assert (t["status"], t["attempts"]) == ("pending", 1)
    assert t["next_attempt_at"] == pytest.approx(T0 + 101)

    assert not process_one(db, h, now=T0 + 100.5, **kw)     # пауза ещё не прошла

    assert process_one(db, h, now=T0 + 101, **kw)           # попытка 2, пауза 2с
    t = task(db)
    assert (t["status"], t["attempts"]) == ("pending", 2)
    assert t["next_attempt_at"] == pytest.approx(T0 + 103)

    assert process_one(db, h, now=T0 + 103, **kw)           # попытка 3 -> failed
    t = task(db)
    assert (t["status"], t["attempts"]) == ("failed", 3)
    assert "boom" in t["last_error"]

    assert not process_one(db, h, now=T0 + 10_000, **kw)    # failed больше не берётся
    assert task(db)["attempts"] == 3


def test_task_succeeds_after_one_failure(client, db):
    send(client)
    h = {"payment_succeeded": FailsOnce()}
    process_one(db, h, now=T0 + 100)
    process_one(db, h, now=T0 + 101)
    t = task(db)
    assert (t["status"], t["attempts"]) == ("success", 2)


def test_unknown_event_type_is_skipped(client, db):
    send(client, type_="something_new")
    assert process_one(db, {"payment_succeeded": ok}, now=T0 + 100)
    assert task(db)["status"] == "skipped"


def test_task_taken_by_dead_worker_is_picked_up_after_lease(client, db):
    send(client)
    claimed = claim_next(db, now=T0 + 0, lease=30)           # воркер взял и "умер"
    assert claimed is not None
    assert not process_one(db, {"payment_succeeded": ok}, now=T0 + 10)   # аренда жива
    assert process_one(db, {"payment_succeeded": ok}, now=T0 + 31)       # истекла -> берём
    t = task(db)
    assert (t["status"], t["attempts"]) == ("success", 2)


def test_worker_dying_on_every_attempt_ends_in_failed_not_pending_forever(client, db):
    send(client)
    for now in (T0, T0 + 31, T0 + 62):                              # три взятия, ни одного результата
        assert claim_next(db, now=now, lease=30) is not None
    assert not process_one(db, {"payment_succeeded": ok}, now=T0 + 93)
    t = task(db)
    assert t["status"] == "failed" and t["attempts"] == 3
    assert "не вернул результат" in t["last_error"]


# ---- /stats ----------------------------------------------------------------

def test_stats(client, db):
    send(client, "e1", "payment_succeeded")
    send(client, "e2", "payment_succeeded")
    send(client, "e3", "something_new")
    send(client, "e3", "something_new")                  # дубликат не считается
    h = {"payment_succeeded": ok}
    while process_one(db, h, now=T0 + 100):
        pass

    data = client.get("/stats").get_json()
    assert data["events_by_type"] == {"payment_succeeded": 2, "something_new": 1}
    assert data["tasks_by_status"] == {"pending": 0, "success": 2, "failed": 0, "skipped": 1}


def test_stats_on_empty_db_lists_all_statuses(client):
    data = client.get("/stats").get_json()
    assert data["events_by_type"] == {}
    assert data["tasks_by_status"] == {"pending": 0, "success": 0, "failed": 0, "skipped": 0}
