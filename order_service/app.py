import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from order_service.api.orders import create_orders_router
from order_service.clients.payment import PaymentClient
from order_service.config import Settings
from order_service.domain.orders import OrderService
from order_service.messaging.kafka_worker import KafkaWorker
from order_service.persistence.database import Database
from order_service.persistence.repositories import OrderRepository, ProductRepository

logger = logging.getLogger(__name__)


def create_app(
    settings: Settings | None = None,
    *,
    start_kafka_worker: bool = True,
    payment_client: PaymentClient | None = None,
) -> FastAPI:
    resolved_settings = settings or Settings.from_environment()
    database = Database(resolved_settings.database_path)
    order_repository = OrderRepository(database)
    product_repository = ProductRepository(database)
    resolved_payment_client = payment_client or PaymentClient(
        resolved_settings.payment_api_url,
        resolved_settings.payment_timeout_seconds,
    )
    order_service = OrderService(product_repository, order_repository, resolved_payment_client)
    worker = KafkaWorker(resolved_settings, order_repository) if start_kafka_worker else None

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        if worker is not None:
            worker.start()
        yield
        if worker is not None:
            worker.stop()
        database.close()

    app = FastAPI(title="Order Service", version="1.0.0", lifespan=lifespan)
    app.state.database = database
    app.include_router(create_orders_router(order_service))

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_: Request, error: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"code": "BadRequest", "message": "Request body is invalid"},
        )

    @app.exception_handler(HTTPException)
    async def http_error_handler(_: Request, error: HTTPException) -> JSONResponse:
        detail = error.detail
        if isinstance(detail, dict):
            body = detail
        else:
            body = {"code": "NotFound" if error.status_code == 404 else "RequestError", "message": str(detail)}
        return JSONResponse(status_code=error.status_code, content=body, headers=error.headers)

    @app.exception_handler(Exception)
    async def unexpected_error_handler(_: Request, error: Exception) -> JSONResponse:
        logger.exception("Unhandled order service error: %s", error)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"code": "InternalServerError", "message": "An unexpected error occurred"},
        )

    return app

app = create_app()
