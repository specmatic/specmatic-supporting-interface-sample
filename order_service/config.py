import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    database_path: str = ":memory:"
    payment_api_url: str = "http://localhost:9000"
    payment_timeout_seconds: float = 5.0
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_receive_topic: str = "payment-status"
    kafka_send_topic: str = "order-status"
    kafka_consumer_group: str = "order-service"
    port: int = 8080

    @classmethod
    def from_environment(cls) -> "Settings":
        return cls(
            database_path=os.getenv("DATABASE_PATH", cls.database_path),
            payment_api_url=os.getenv("PAYMENT_API_URL", cls.payment_api_url),
            payment_timeout_seconds=float(os.getenv("PAYMENT_TIMEOUT_SECONDS", str(cls.payment_timeout_seconds))),
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", cls.kafka_bootstrap_servers),
            kafka_receive_topic=os.getenv("KAFKA_RECEIVE_TOPIC", cls.kafka_receive_topic),
            kafka_send_topic=os.getenv("KAFKA_SEND_TOPIC", cls.kafka_send_topic),
            kafka_consumer_group=os.getenv("KAFKA_CONSUMER_GROUP", cls.kafka_consumer_group),
            port=int(os.getenv("PORT", str(cls.port))),
        )
