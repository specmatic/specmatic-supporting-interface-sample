import httpx


class PaymentServiceUnavailable(Exception):
    """The payment service could not be reached."""


class PaymentServiceRejected(Exception):
    """The payment service did not accept the initiation request."""


class PaymentClient:
    def __init__(self, base_url: str, timeout_seconds: float) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def initiate_payment(self, idempotency_key: str, price: float) -> None:
        try:
            response = httpx.post(
                f"{self.base_url}/payment/initiate",
                headers={"Idempotency-Key": idempotency_key},
                json={"price": price},
                timeout=self.timeout_seconds,
            )
        except httpx.RequestError as error:
            raise PaymentServiceUnavailable from error
        if response.status_code != 202:
            raise PaymentServiceRejected(f"Payment service returned HTTP {response.status_code}")
