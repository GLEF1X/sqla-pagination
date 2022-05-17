import contextlib
from typing import Sequence, Optional, Dict, Any

from sqlalchemy import func, select
from sqlalchemy.engine import Row

from sqlapagination.constants import DEFAULT_PAGE_SIZE
from sqlapagination.page import AbstractPage
from sqlapagination.paginators.base import Paginator, R, SelectOrQuery, P
from sqlapagination.paginators.limit_offset.page import LimitOffsetPage


class LimitOffsetPaginator(Paginator):

    def __init__(
            self,
            query_or_select: SelectOrQuery,
            page_size: int = DEFAULT_PAGE_SIZE,
            bookmark: Optional[Dict[str, Any]] = None,
            total_count_key: str = "sqlalchemy_pagination_total_count",
    ):
        super().__init__(query_or_select, page_size, bookmark)
        self._offset = self._bookmark.get("offset", 0)
        self._total_count_key = total_count_key

    def get_modified_sql_statement(self) -> SelectOrQuery:
        return self._with_total_count_subquery(
            self._select_or_query.limit(self._page_size).offset(self._offset)
        )

    @contextlib.contextmanager
    def bookmarked(self: P, bookmark: Dict[str, Any]) -> P:
        self._bookmark = bookmark
        try:
            self._offset = bookmark.get("offset", 0)
            yield self
        finally:
            self._offset = 0
            self._bookmark = {}

    def _with_total_count_subquery(self, stmt: SelectOrQuery) -> SelectOrQuery:
        return stmt.add_columns(
            select(
                func.count("*")
            ).select_from(self._select_or_query.get_final_froms()[0]).scalar_subquery().label(
                self._total_count_key
            )
        )

    def parse_result(self, resulted_rows: Sequence[Row]) -> AbstractPage[R]:
        if not resulted_rows:
            return LimitOffsetPage([], self._page_size, self._offset)

        total_rows_count: int = resulted_rows[0][self._total_count_key]

        return LimitOffsetPage(
            [r[0] for r in resulted_rows],
            self._page_size,
            self._offset,
            total_rows_count
        )
