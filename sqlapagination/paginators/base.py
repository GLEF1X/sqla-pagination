import abc
from typing import TypeVar, Optional, Any, Generic, Union, Sequence, Dict

from sqlalchemy.engine import Row
from sqlalchemy.orm import Query
from sqlalchemy.sql import Select

from sqlapagination.constants import DEFAULT_PAGE_SIZE
from sqlapagination.page import AbstractPage

SelectOrQuery = TypeVar('SelectOrQuery', bound=Union[Query, Select])
ModifyMetadata = Any
R = TypeVar('R')
P = TypeVar('P')


class Paginator(abc.ABC, Generic[R]):

    def __init__(
            self,
            query_or_select: SelectOrQuery,
            page_size: int = DEFAULT_PAGE_SIZE,
            bookmark: Optional[Dict[str, Any]] = None
    ):
        self._page_size = page_size
        self._select_or_query = query_or_select

        if bookmark is None:
            bookmark = {}
        self._bookmark = bookmark

    @abc.abstractmethod
    def get_modified_sql_statement(self) -> SelectOrQuery:
        pass

    @abc.abstractmethod
    def bookmarked(self: P, bookmark: Dict[str, Any]) -> P:
        pass

    @abc.abstractmethod
    def parse_result(self, resulted_rows: Sequence[Row]) -> AbstractPage[R]:
        pass
