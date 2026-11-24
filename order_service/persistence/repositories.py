from order_service.domain.models import Order, Product
from order_service.persistence.database import Database


class ProductRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    def get_by_id(self, product_id: int) -> Product | None:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT product_id, name, unit_price FROM products WHERE product_id = ?",
                (product_id,),
            ).fetchone()
        if row is None:
            return None
        return Product(product_id=row["product_id"], name=row["name"], unit_price=row["unit_price"])


class OrderRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    def create_or_get(
        self,
        correlation_id: str,
        idempotency_key: str,
        product_id: int,
        quantity: int,
        price: float,
    ) -> tuple[Order, bool]:
        with self.database.connect() as connection:
            existing = connection.execute(
                "SELECT order_id, correlation_id, product_id, quantity, price, payment_initiated "
                "FROM orders WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if existing is not None:
                return self._order_from_row(existing), not bool(existing["payment_initiated"])

            cursor = connection.execute(
                "INSERT INTO orders (correlation_id, idempotency_key, product_id, quantity, price) "
                "VALUES (?, ?, ?, ?, ?)",
                (correlation_id, idempotency_key, product_id, quantity, price),
            )
            order_id = cursor.lastrowid
        return Order(order_id, correlation_id, product_id, quantity, price), True

    def get_by_correlation_id(self, correlation_id: str) -> Order | None:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT order_id, correlation_id, product_id, quantity, price "
                "FROM orders WHERE correlation_id = ?",
                (correlation_id,),
            ).fetchone()
        return self._order_from_row(row) if row is not None else None

    def mark_payment_initiated(self, correlation_id: str) -> None:
        with self.database.connect() as connection:
            connection.execute(
                "UPDATE orders SET payment_initiated = 1 WHERE correlation_id = ?",
                (correlation_id,),
            )

    def complete(
        self,
        correlation_id: str,
        payment_id: str,
        status: str,
        failure_reason: str | None,
    ) -> None:
        with self.database.connect() as connection:
            connection.execute(
                "UPDATE orders SET payment_id = ?, status = ?, failure_reason = ? WHERE correlation_id = ?",
                (payment_id, status, failure_reason, correlation_id),
            )

    @staticmethod
    def _order_from_row(row) -> Order:
        return Order(
            order_id=row["order_id"],
            correlation_id=row["correlation_id"],
            product_id=row["product_id"],
            quantity=row["quantity"],
            price=row["price"],
        )
