from typing import Optional, Dict, Any, List

from sqlalchemy import select, Table, and_, Column

from sqlapagination.paginators.limit_offset.paginator import LimitOffsetPaginator
from sqlapagination.constants import DEFAULT_PAGE_SIZE
from sqlapagination.paginators.base import SelectOrQuery


class JoinBasedPaginator(LimitOffsetPaginator):

    def __init__(
            self,
            query_or_select: SelectOrQuery,
            page_size: int = DEFAULT_PAGE_SIZE,
            bookmark: Optional[Dict[str, Any]] = None
    ) -> None:
        super().__init__(query_or_select, page_size, bookmark)
        self._offset = self._bookmark.get("offset", 0)

    def get_modified_sql_statement(self) -> SelectOrQuery:
        first_from: Table = self._select_or_query.get_final_froms()[0]
        primary_key_constraint = first_from.primary_key

        primary_keys: List[Column] = [column for column in primary_key_constraint.columns]
        subselect = select(*primary_keys).select_from(first_from).order_by(
            *primary_keys
        ).limit(self._page_size).offset(self._offset).subquery()

        join_clause = [c == s for c, s in zip(primary_key_constraint.columns, subselect.columns)]

        return self._with_total_count_subquery(self._select_or_query).join(
            subselect,
            onclause=and_(*join_clause)
        )
