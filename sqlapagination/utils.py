from typing import List, Any, Sequence, Union, TypeVar

from sqlalchemy.engine import Connection, Row, Dialect
from sqlalchemy.ext.asyncio import AsyncSession, AsyncConnection
from sqlalchemy.orm import Query, ColumnProperty, Session
from sqlalchemy.sql import Select

T = TypeVar('T')


def get_value_from_row_by_column_name(row: Any, column_name: str) -> Any:
    if isinstance(row, Row):
        row = row._mapping()
        return row[column_name]

    return getattr(row, column_name)


def unpack_rows_if_row_contains_only_orm_model(rows: Sequence[T]) -> List[T]:
    unpacked = []

    for row in rows:
        if len(row.keys()) != 1:
            unpacked.append(row)
            continue

        unpacked.append(row[0])

    return unpacked


def get_column_descriptors(selectable_or_query: Union[Query, Select]) -> List[ColumnProperty]:
    return selectable_or_query.column_descriptions


def get_db_dialect(
        connection_or_session: Union[Connection, Session, AsyncSession, AsyncConnection]
) -> Dialect:
    if isinstance(connection_or_session, (Session, AsyncSession)):
        return connection_or_session.get_bind().dialect

    return connection_or_session.dialect


def get_order_by_clauses(selectable: Select) -> Sequence[Any]:
    return selectable._order_by_clauses


def get_group_by_clauses(selectable: Select) -> Sequence[Any]:
    return selectable._group_by_clauses
