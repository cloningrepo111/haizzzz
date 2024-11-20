from typing import Union

from pydantic import BaseModel, model_validator, Field

from src.models import *


class Range(BaseModel):
    start: Union[str, int]
    end: Union[str, int]

    @model_validator(mode='after')
    def compare_type(self) -> 'Range':
        if type(self.start) is not type(self.end):
            raise ValueError("Range value must be same type")
        return self


ItemOperator = Annotated[str, StringConstraints(pattern="^(is|is_not|is_one_of|is_not_one_of|exist|not_exist|less_than|greater_or_equal|between|not_between)$")]
QueryOperator = Annotated[str, StringConstraints(pattern="^(or|and)$")]
Value = Union[str, int, bool, list[str], list[int], Range, None]


class FilterItem(BaseModel):
    operator: ItemOperator
    key: StrictStr
    value: Value = Field(default=None)

    @model_validator(mode='after')
    def validate_item_query(self) -> 'FilterItem':
        match self.operator:
            case "is" | "is_not":
                if not isinstance(self.value, (str, int, bool)):
                    raise ValueError("Invalid value")
            case "is_one_of" | "is_not_one_of":
                if not isinstance(self.value, list):
                    raise ValueError("Value must be a valid list")
                if not all(isinstance(item, str) for item in self.value) and \
                        not all(isinstance(item, int) for item in self.value):
                    raise ValueError("Invalid array value")
            case "less_than" | "greater_or_equal":
                if not isinstance(self.value, (str, int)):
                    raise ValueError("Invalid value")
            case "between" | "not_between":
                if not isinstance(self.value, Range):
                    raise ValueError("Invalid range value")
            case "exist" | "not_exist":
                self.value = None
            case _:
                raise ValueError("Invalid operator")
        return self


class Filter(BaseModel):
    operator: QueryOperator = Field(default="and")
    items: list['FilterItem']
    nested: list['Filter']
    include: bool = True

    @model_validator(mode='after')
    def validate_query(self) -> 'Filter':
        if not all(item.operator != self.operator for item in self.nested):
            raise ValueError("Uncorrected input filters")
        return self


class CoreQuery(BaseModel):
    queries: list[dict]
    filters: list[Filter]
    includes: list[StrictStr]
    excludes: list[StrictStr]
    ids: list[StrictStr]

    @model_validator(mode='after')
    def validate_query(self) -> 'CoreQuery':
        self.queries = [item for item in self.queries if item]
        return self

