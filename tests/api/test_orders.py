from pathlib import Path
from sqlite3 import OperationalError
from unittest.mock import patch

from fastapi.testclient import TestClient

from order_service.app import create_app
from order_service.config import Settings


class FakePaymentClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, float]] = []

    def initiate_payment(self, idempotency_key: str, price: float) -> None:
        self.calls.append((idempotency_key, price))


def make_settings(database_path: Path) -> Settings:
    return Settings(database_path=str(database_path))


def test_create_order_charges_unit_price_times_quantity_and_returns_correlation_id(tmp_path: Path) -> None:
    payment = FakePaymentClient()
    app = create_app(make_settings(tmp_path / "orders.db"), start_kafka_worker=False, payment_client=payment)

    with TestClient(app) as client:
        response = client.post(
            "/orders",
            headers={"Idempotency-Key": "demo-order-1"},
            json={"productId": 10, "quantity": 50},
        )

    assert response.status_code == 202
    correlation_id = response.headers["Correlation-Id"]
    assert correlation_id == "payment-order-demo-order-1"
    assert payment.calls == [(correlation_id, 9975.0)]


def test_create_order_returns_404_for_unknown_product(tmp_path: Path) -> None:
    payment = FakePaymentClient()
    app = create_app(make_settings(tmp_path / "orders.db"), start_kafka_worker=False, payment_client=payment)

    with TestClient(app) as client:
        response = client.post(
            "/orders",
            headers={"Idempotency-Key": "unknown-product"},
            json={"productId": 999, "quantity": 1},
        )

    assert response.status_code == 404
    assert response.json() == {"code": "ProductNotFound", "message": "Product 999 was not found"}
    assert payment.calls == []


def test_idempotency_key_reuses_the_existing_order(tmp_path: Path) -> None:
    payment = FakePaymentClient()
    app = create_app(make_settings(tmp_path / "orders.db"), start_kafka_worker=False, payment_client=payment)

    with TestClient(app) as client:
        first = client.post(
            "/orders",
            headers={"Idempotency-Key": "repeat-order"},
            json={"productId": 10, "quantity": 2},
        )

        second = client.post(
            "/orders",
            headers={"Idempotency-Key": "repeat-order"},
            json={"productId": 10, "quantity": 50},
        )

    assert first.status_code == 202
    assert second.status_code == 202
    assert first.headers["Correlation-Id"] == second.headers["Correlation-Id"]
    assert first.headers["Correlation-Id"] == "payment-order-repeat-order"
    assert payment.calls == [(first.headers["Correlation-Id"], 399.0)]


def test_invalid_quantity_returns_400(tmp_path: Path) -> None:
    payment = FakePaymentClient()
    app = create_app(make_settings(tmp_path / "orders.db"), start_kafka_worker=False, payment_client=payment)
    with TestClient(app) as client:
        response = client.post(
            "/orders",
            headers={"Idempotency-Key": "invalid-quantity"},
            json={"productId": 10, "quantity": 0},
        )

    assert response.status_code == 400
    assert response.json() == {"code": "BadRequest", "message": "Request body is invalid"}


def test_create_order_returns_500_for_unexpected_storage_failure(tmp_path: Path) -> None:
    payment = FakePaymentClient()
    app = create_app(make_settings(tmp_path / "orders.db"), start_kafka_worker=False, payment_client=payment)

    with (
        TestClient(app, raise_server_exceptions=False) as client,
        patch.object(app.state.database, "connect", side_effect=OperationalError("Storage unavailable")),
    ):
        response = client.post(
            "/orders",
            headers={"Idempotency-Key": "storage-error"},
            json={"productId": 10, "quantity": 1},
        )

    assert response.status_code == 500
    assert response.json() == {"code": "InternalServerError", "message": "An unexpected error occurred"}
    assert payment.calls == []
