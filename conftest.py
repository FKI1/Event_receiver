import pytest

from eventsvc import create_app
from eventsvc.db import connect


@pytest.fixture
def app(tmp_path):
    return create_app(str(tmp_path / "test.db"))


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def db(app):
    conn = connect(app.config["DB_PATH"])
    yield conn
    conn.close()
