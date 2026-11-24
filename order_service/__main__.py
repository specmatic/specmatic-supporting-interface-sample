import uvicorn

from order_service.config import Settings


def main() -> None:
    settings = Settings.from_environment()
    uvicorn.run("order_service.app:app", host="0.0.0.0", port=settings.port)


if __name__ == "__main__":
    main()
