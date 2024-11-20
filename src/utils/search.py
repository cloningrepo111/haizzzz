import uuid

from bson import ObjectId
from src.db import SearchStage, SearchPipeline, SearchResult

from src.models.response_schema import ResponseModel


async def generate_id() -> str:
    return str(uuid.uuid4())


