"""Заглушки обработчиков: по типу события что-то делают и иногда падают."""
import logging
import os
import random

log = logging.getLogger(__name__)


class SimulatedError(Exception):
    pass


def _maybe_fail(what):
    rate = float(os.environ.get("FAIL_RATE", "0.3"))
    if random.random() < rate:
        raise SimulatedError(f"имитация сбоя при обработке {what}")


def payment_succeeded(payload):
    _maybe_fail("payment_succeeded")
    log.info("payment_succeeded: зачисляем платёж %s", payload)


def payment_failed(payload):
    _maybe_fail("payment_failed")
    log.info("payment_failed: уведомляем клиента %s", payload)


def user_registered(payload):
    _maybe_fail("user_registered")
    log.info("user_registered: отправляем приветствие %s", payload)


# Тип события без обработчика -> задача получает статус skipped.
HANDLERS = {
    "payment_succeeded": payment_succeeded,
    "payment_failed": payment_failed,
    "user_registered": user_registered,
}
