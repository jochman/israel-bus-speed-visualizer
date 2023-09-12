from __future__ import annotations
import pandas as pd
from pydantic import BaseModel


class Filter(BaseModel):
    name: str
    value: str
    filter: list["Filter"] = []

def _filter(data: pd.DataFrame, filter: Filter):
    filtered = data.loc[data[filter.name] == filter.value]
    for filter_ in filter.filter:
        filtered 
def filter(data: pd.DataFrame, filters: list[Filter]):
