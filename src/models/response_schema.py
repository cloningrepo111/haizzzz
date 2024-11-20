from typing import Any

from pydantic import BaseModel


class ResponseModel(BaseModel):
    status: bool
    data: Any = None
    message: str