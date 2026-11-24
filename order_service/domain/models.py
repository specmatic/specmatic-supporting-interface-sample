from dataclasses import dataclass


@dataclass(frozen=True)
class Product:
    product_id: int
    name: str
    unit_price: float


@dataclass(frozen=True)
class Order:
    order_id: int
    correlation_id: str
    product_id: int
    quantity: int
    price: float
