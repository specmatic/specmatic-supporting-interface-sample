from order_service.clients.payment import PaymentClient
from order_service.domain.models import Order
from order_service.persistence.repositories import OrderRepository, ProductRepository


class ProductNotFoundError(Exception):
    """The local catalog does not contain the requested product."""


class OrderService:
    def __init__(
        self,
        products: ProductRepository,
        orders: OrderRepository,
        payments: PaymentClient,
    ) -> None:
        self.products = products
        self.orders = orders
        self.payments = payments

    def create_order(self, product_id: int, quantity: int, idempotency_key: str) -> Order:
        product = self.products.get_by_id(product_id)
        if product is None:
            raise ProductNotFoundError(product_id)

        total_price = round(product.unit_price * quantity, 2)
        correlation_id = f"payment-order-{idempotency_key}"
        order, should_initiate_payment = self.orders.create_or_get(
            correlation_id,
            idempotency_key,
            product.product_id,
            quantity,
            total_price,
        )

        if should_initiate_payment:
            self.payments.initiate_payment(order.correlation_id, order.price)
            self.orders.mark_payment_initiated(order.correlation_id)

        return order
