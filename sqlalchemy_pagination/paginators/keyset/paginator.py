from dataclasses import dataclass, field
from typing import Optional, Generic, Sequence, List, Any, Dict

from sqlalchemy import tuple_, Column
from sqlalchemy.engine import Row
from sqlalchemy.sql.elements import BooleanClauseList, or_, and_, UnaryExpression

from sqlalchemy_pagination.constants import DEFAULT_PAGE_SIZE
from sqlalchemy_pagination.exceptions import KeySetPairsMismatchQueryError
from sqlalchemy_pagination.page import AbstractPage, T
from sqlalchemy_pagination.paginators.base import Paginator, SelectOrQuery
from sqlalchemy_pagination.paginators.keyset.page import KeySetPage
from sqlalchemy_pagination.paginators.keyset.utils.ordering import parse_order_by_clause, find_order_key, \
    OrderByColumnWrapper
from sqlalchemy_pagination.utils import get_column_descriptors, get_group_by_clauses, unpack_rows_if_row_contains_only_orm_model

SUPPORTS_NATIVE_ROW_VALUES_COMPARISON = {"postgresql", "mysql", "sqlite"}


@dataclass
class ParsingMetadata:
    page_size: int = DEFAULT_PAGE_SIZE
    order_by_columns: List[OrderByColumnWrapper] = field(default_factory=list)
    bookmark: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.expected_entries_count = self.page_size + 1


class KeySetPaginator(Paginator[T], Generic[T]):

    # TODO before keyset and after keyset values
    def __init__(
            self,
            query_or_select: SelectOrQuery,
            page_size: int = DEFAULT_PAGE_SIZE,
            dialect: Optional[Any] = None,
            bookmark: Optional[Dict[str, Any]] = None
    ) -> None:
        super().__init__(query_or_select, page_size, bookmark)
        self._order_by_columns = parse_order_by_clause(self._select_or_query)
        if self._should_reverse_order_by_clause():
            self._order_by_columns = [c.reversed for c in self._order_by_columns]

        self._order_by_clauses: List[UnaryExpression] = self._scaffold_order_by_clauses()
        self._dialect = dialect

    def _scaffold_order_by_clauses(self) -> List[UnaryExpression]:
        column_descriptors = get_column_descriptors(self._select_or_query)
        mapped_order_by_columns = [find_order_key(c, column_descriptors) for c in self._order_by_columns]

        return [column.order_by_clause for column in mapped_order_by_columns]

    def _should_reverse_order_by_clause(self) -> bool:
        if self._bookmark.get("direction", "forward") == "backward":
            return True

        return False

    def get_modified_sql_statement(self) -> SelectOrQuery:
        select_or_query = self._apply_filter_condition_if_required().order_by(
            None
        ).order_by(*self._order_by_clauses)
        total_rows_num_plus_one_extra_to_check_next = self._page_size + 1
        select_or_query = select_or_query.limit(total_rows_num_plus_one_extra_to_check_next)

        return select_or_query

    def _apply_filter_condition_if_required(self) -> SelectOrQuery:
        if not self._bookmark:
            return self._select_or_query

        group_by_clauses = get_group_by_clauses(self._select_or_query)
        keyset_pairs: Dict[str, Any] = self._bookmark["keyset_pairs"]

        order_by_column_names = [c.name for c in self._order_by_columns]
        if order_by_column_names != list(keyset_pairs.keys()):
            raise KeySetPairsMismatchQueryError(
                "Order by columns are not equal to keyset pairs\n"
                f"{list(keyset_pairs.keys())} != {order_by_column_names}"
            )

        zipped = zip(self._order_by_columns, keyset_pairs.values())
        swapped = [
            c.pair_for_comparison(value, self._dialect)
            for c, value in zipped
        ]
        greater_row, lesser_row = zip(*swapped)

        filter_condition = self._compare_sql_row_values(greater=greater_row, lesser=lesser_row)

        if group_by_clauses is not None and len(group_by_clauses) > 0:
            return self._select_or_query.having(filter_condition)

        return self._select_or_query.where(filter_condition)

    def _compare_sql_row_values(self, lesser: List[Column], greater: List[Column]) -> BooleanClauseList:
        if len(lesser) != len(greater):
            raise ValueError("Tuples must have same length to be compared!")

        if len(lesser) == 1:
            return lesser[0] < greater[0]

        if self._dialect is not None and self._dialect.name.lower() in SUPPORTS_NATIVE_ROW_VALUES_COMPARISON:
            return tuple_(*lesser) < tuple_(*greater)

        return or_(
            *[
                and_(
                    *[lesser[index] == greater[index] for index in range(eq_depth)],
                    lesser[eq_depth] < greater[eq_depth]
                )
                for eq_depth in range(len(lesser))
            ]
        )

    def parse_result(
            self,
            resulted_rows: Sequence[Row]
    ) -> AbstractPage[T]:
        return KeySetPage(
            rows=unpack_rows_if_row_contains_only_orm_model(resulted_rows),
            parsing_metadata=ParsingMetadata(
                page_size=self._page_size,
                order_by_columns=self._order_by_columns,
                bookmark=self._bookmark
            )
        )
