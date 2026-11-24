from pydantic import BaseModel, Field


class CreateOrderRequest(BaseModel):
    product_id: int = Field(alias="productId", strict=True)
    quantity: int = Field(ge=1, strict=True)
