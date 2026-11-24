from unittest.mock import patch

from order_service.clients.payment import PaymentClient


def test_payment_client_sends_the_single_prefixed_idempotency_header() -> None:
    with patch("order_service.clients.payment.httpx.post") as post:
        post.return_value.status_code = 202
        PaymentClient("http://payment.local", timeout_seconds=5).initiate_payment(
            "payment-order-1234",
            9975.0,
        )

    post.assert_called_once_with(
        "http://payment.local/payment/initiate",
        headers={"Idempotency-Key": "payment-order-1234"},
        json={"price": 9975.0},
        timeout=5,
    )
