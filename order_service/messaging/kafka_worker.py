import json
import logging
from threading import Event, Thread
from typing import Any

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from kafka.structs import TopicPartition

from order_service.config import Settings
from order_service.persistence.repositories import OrderRepository

logger = logging.getLogger(__name__)


def correlation_id_from_headers(headers: list[tuple[str, bytes | None]] | None) -> str | None:
    for key, value in headers or []:
        if key.casefold() == "correlation-id" and value:
            try:
                return value.decode("utf-8")
            except UnicodeDecodeError:
                return None
    return None


def deserialize_json(value: bytes | None) -> Any:
    if value is None:
        return None
    try:
        return json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        logger.warning("Skipping malformed JSON Kafka message")
        return None


def process_payment_status(message: Any, orders: OrderRepository, producer: Any, send_topic: str) -> bool:
    correlation_id = correlation_id_from_headers(message.headers)
    payload = message.value
    if not correlation_id or not isinstance(payload, dict):
        logger.warning("Skipping payment status message without a correlation ID or object payload")
        return True

    order = orders.get_by_correlation_id(correlation_id)
    if order is None:
        logger.warning("Skipping payment status for unknown correlation ID %s", correlation_id)
        return True

    payment_id = payload.get("paymentId")
    payment_status = payload.get("status")
    if (
        not isinstance(payment_id, str)
        or not isinstance(payment_status, str)
        or payment_status not in {"success", "failed"}
    ):
        logger.warning("Skipping invalid payment status for correlation ID %s", correlation_id)
        return True

    raw_failure_reason = payload.get("failureReason")
    if raw_failure_reason is not None and not isinstance(raw_failure_reason, str):
        logger.warning("Skipping invalid failure reason for correlation ID %s", correlation_id)
        return True

    order_status = {
        "orderId": str(order.order_id),
        "paymentId": payment_id,
        "status": payment_status,
        "productId": str(order.product_id),
        "quantity": order.quantity,
        "price": order.price,
    }
    failure_reason = raw_failure_reason
    if payment_status == "failed" and isinstance(failure_reason, str):
        order_status["failureReason"] = failure_reason

    try:
        producer.send(
            send_topic,
            value=order_status,
            headers=[("Correlation-Id", correlation_id.encode("utf-8"))],
        ).get(timeout=10)
        producer.flush()
    except KafkaError:
        logger.exception("Could not publish order status for correlation ID %s", correlation_id)
        return False

    orders.complete(correlation_id, payment_id, payment_status, failure_reason)
    logger.info("Published order status for correlation ID %s", correlation_id)
    return True


class KafkaWorker:
    def __init__(self, settings: Settings, orders: OrderRepository) -> None:
        self.settings = settings
        self.orders = orders
        self.stop_event = Event()
        self.thread = Thread(target=self._run, name="payment-status-consumer", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=10)

    def _run(self) -> None:
        retry_delay_seconds = 5
        while not self.stop_event.is_set():
            consumer = None
            producer = None
            try:
                consumer = KafkaConsumer(
                    self.settings.kafka_receive_topic,
                    bootstrap_servers=self.settings.kafka_bootstrap_servers.split(","),
                    group_id=self.settings.kafka_consumer_group,
                    enable_auto_commit=False,
                    auto_offset_reset="earliest",
                    value_deserializer=deserialize_json,
                )
                producer = KafkaProducer(
                    bootstrap_servers=self.settings.kafka_bootstrap_servers.split(","),
                    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                )
                logger.info("Consuming payment status messages from %s", self.settings.kafka_receive_topic)
                while not self.stop_event.is_set():
                    records = consumer.poll(timeout_ms=500, max_records=1)
                    for messages in records.values():
                        for message in messages:
                            if process_payment_status(message, self.orders, producer, self.settings.kafka_send_topic):
                                consumer.commit()
                            else:
                                consumer.seek(TopicPartition(message.topic, message.partition), message.offset)
                                self.stop_event.wait(1)
            except KafkaError as error:
                if not self.stop_event.is_set():
                    logger.warning(
                        "Kafka payment status worker could not reach the broker; retrying in %s seconds: %s",
                        retry_delay_seconds,
                        error,
                    )
            except Exception:
                if not self.stop_event.is_set():
                    logger.exception("Kafka payment status worker failed; retrying in %s seconds", retry_delay_seconds)
            finally:
                for client in (consumer, producer):
                    if client is not None:
                        try:
                            client.close()
                        except KafkaError as error:
                            logger.debug("Could not close a Kafka client cleanly: %s", error)
                        except Exception as error:
                            logger.warning("Could not close a Kafka client cleanly: %s", error)

            if not self.stop_event.is_set():
                self.stop_event.wait(retry_delay_seconds)
