"""
Inspects performance issues with indexes.
"""
from typing import Union

from sqlalchemy.orm import Query
from sqlalchemy.sql import Select

# TODO index inspection API / cli


def has_index_for_order_by_columns(select_or_query: Union[Select, Query]) -> bool:
    pass


def has_index_for_where_clause_columns() -> bool:
    pass


def is_order_of_index_columns_is_most_appropriate() -> bool:
    pass
