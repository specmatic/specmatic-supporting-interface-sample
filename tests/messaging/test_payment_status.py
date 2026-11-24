from types import SimpleNamespace

import pytest

from order_service.messaging.kafka_worker import process_payment_status
from order_service.persistence.database import Database
from order_service.persistence.repositories import OrderRepository


class FakeProducer:
    def __init__(self) -> None:
        self.messages: list[dict] = []
        self.flushed = False

    def send(self, topic: str, value: dict, headers: list[tuple[str, bytes]]) -> "FakeRecordMetadata":
        self.messages.append({"topic": topic, "value": value, "headers": headers})
        return FakeRecordMetadata()

    def flush(self) -> None:
        self.flushed = True


class FakeRecordMetadata:
    def get(self, timeout: int) -> None:
        assert timeout == 10


def test_payment_status_is_correlated_and_published_as_order_status() -> None:
    database = Database(":memory:")
    orders = OrderRepository(database)
    correlation_id = "payment-order-1234"
    order, created = orders.create_or_get(correlation_id, "1234", 10, 2, 399.0)
    assert created
    producer = FakeProducer()
    message = SimpleNamespace(
        headers=[("Correlation-Id", correlation_id.encode("utf-8"))],
        value={"paymentId": "payment-123", "status": "failed", "failureReason": "declined"},
    )

    processed = process_payment_status(message, orders, producer, "order-status")

    assert processed
    assert producer.flushed
    assert producer.messages == [
        {
            "topic": "order-status",
            "value": {
                "orderId": str(order.order_id),
                "paymentId": "payment-123",
                "status": "failed",
                "productId": "10",
                "quantity": 2,
                "price": 399.0,
                "failureReason": "declined",
            },
            "headers": [("Correlation-Id", correlation_id.encode("utf-8"))],
        },
    ]
    database.close()


@pytest.mark.parametrize("status", [[], {}, None, "pending"], ids=["array", "object", "null", "unknown"])
def test_invalid_payment_status_is_handled_without_publishing(status: object) -> None:
    database = Database(":memory:")
    orders = OrderRepository(database)
    correlation_id = "payment-order-1234"
    producer = FakeProducer()
    message = SimpleNamespace(
        headers=[("Correlation-Id", correlation_id.encode("utf-8"))],
        value={"paymentId": "payment-123", "status": status},
    )

    try:
        orders.create_or_get(correlation_id, "1234", 10, 2, 399.0)
        processed = process_payment_status(message, orders, producer, "order-status")
    finally:
        database.close()

    assert processed
    assert producer.messages == []
    assert producer.flushed is False
