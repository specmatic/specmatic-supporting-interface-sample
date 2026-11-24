from fastapi import APIRouter, Header, HTTPException, Response, status

from order_service.api.schemas import CreateOrderRequest
from order_service.clients.payment import PaymentServiceRejected, PaymentServiceUnavailable
from order_service.domain.orders import OrderService, ProductNotFoundError


def create_orders_router(order_service: OrderService) -> APIRouter:
    router = APIRouter()

    @router.post("/orders", status_code=status.HTTP_202_ACCEPTED)
    def create_order(
        order_request: CreateOrderRequest,
        idempotency_key: str = Header(alias="Idempotency-Key"),
    ) -> Response:
        try:
            order = order_service.create_order(
                order_request.product_id,
                order_request.quantity,
                idempotency_key,
            )
        except ProductNotFoundError as error:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "code": "ProductNotFound",
                    "message": f"Product {error.args[0]} was not found",
                },
            ) from error
        except PaymentServiceUnavailable as error:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={"code": "PaymentServiceUnavailable", "message": "Payment service is unavailable"},
            ) from error
        except PaymentServiceRejected as error:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail={"code": "PaymentServiceError", "message": "Payment service did not accept the order"},
            ) from error

        return Response(
            status_code=status.HTTP_202_ACCEPTED,
            headers={"Correlation-Id": order.correlation_id},
        )

    return router
