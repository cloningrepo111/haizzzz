from datetime import datetime

from pydantic import BaseModel, Field, model_validator


class RelativeTime(BaseModel):
    time: int = Field(default=1, ge=1)
    unit: str = Field(default="m", pattern="^(m|h|d)$")


class AbsoluteTime(BaseModel):
    start: datetime
    end: datetime

    @model_validator(mode='after')
    def validate_absolute_model(self) -> 'AbsoluteTime':
        if self.start.timestamp() > self.end.timestamp():
            raise ValueError("End must br greater than start time")
        return self


class TimeModel(BaseModel):
    relative: RelativeTime | None = None
    absolute: AbsoluteTime | None = None
    get_relative: bool = True

    @model_validator(mode='after')
    def validate_timemodel(self) -> 'TimeModel':
        if self.get_relative:
            if not self.relative:
                raise ValueError("Missing relative time config")
        else:
            if not self.absolute:
                raise ValueError("Missing absolute time config")
        return self
