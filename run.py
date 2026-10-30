import logging

from eventsvc import create_app
from eventsvc.handlers import HANDLERS
from eventsvc.worker import Worker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
app = create_app()

if __name__ == "__main__":
    Worker(app.config["DB_PATH"], HANDLERS).start()
    app.run(port=5000, use_reloader=False)
