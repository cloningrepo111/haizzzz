from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict

from pydantic import BaseModel, Field

from src.models import *
from src.models.query_schema import CoreQuery
from src.models.time_schema import TimeModel


class SearchModel(BaseModel):
    query: CoreQuery | None = None
    limit: int = Field(default=500, ge=1, le=5000)
    sorted: str = Field(pattern="^(@timestamp)$", default="@timestamp")
    order: str = Field(pattern="^(asc|desc)$", default="desc")
    after: Optional[int] = None
    compress: bool = False


class StageModel(SearchModel):
    id: StrictStr | None = None
    name: StrictStr | None = None
    saved: bool = False


class CreateStageModel(StageModel):
    pipeline_id: str
    created_by: dict | None = None
    created_at: datetime | None = datetime.now(timezone.utc)


class UpdateStageModel(StageModel):
    updated_by: dict | None = None
    updated_at: datetime | None = datetime.now(timezone.utc)


class PipelineModel(BaseModel):
    name: str | None = None
    saved: bool = False
    time: TimeModel = None
    source: List[StrictStr] = Field(default=[])
    tenant: List[StrictStr] = Field(default=[])
    stage_ids: List[StrictStr] = Field(default=[])


class CreatePipelineModel(PipelineModel):
    created_by: dict | None = None
    created_at: datetime | None = datetime.now(timezone.utc)


class UpdatePipelineModel(PipelineModel):
    id: str
    updated_by: dict | None = None
    updated_at: datetime | None = datetime.now(timezone.utc)

