from typing import Generic, TypeVar, Optional, List
from pydantic import BaseModel

T = TypeVar("T")


class ResponseBase(BaseModel):
    success: bool
    message: str
    

class DataResponse(ResponseBase, Generic[T]):
    data: Optional[T] = None


class PaginationResponse(ResponseBase, Generic[T]):
    data: List[T]
    total: int
    page: int
    size: int
    pages: int