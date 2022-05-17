"""
Internal API for ordering. Copied directly from sqlakeyset source code.
"""

import abc
from copy import copy
from typing import Any, List, Callable, Optional, Tuple, Union, Sequence
from warnings import warn

import sqlalchemy
from sqlalchemy import asc, column, Column
from sqlalchemy.engine import Row
from sqlalchemy.exc import ArgumentError
from sqlalchemy.orm import Bundle, Mapper, class_mapper, Query
from sqlalchemy.orm.attributes import QueryableAttribute
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import _label_reference, ClauseElement
from sqlalchemy.sql.expression import ClauseList, ColumnElement, Label
from sqlalchemy.sql.operators import asc_op, desc_op, nullsfirst_op, nullslast_op

from sqlapagination.constants import ORDER_COL_PREFIX
from sqlapagination.paginators.keyset.inspection.columns import warn_if_column_nullable
from sqlapagination.utils import get_order_by_clauses

_LABELLED = (Label, _label_reference)
_ORDER_MODIFIERS = (asc_op, desc_op, nullsfirst_op, nullslast_op)
_UNSUPPORTED_ORDER_MODIFIERS = (nullsfirst_op, nullslast_op)
_WRAPPING_DEPTH = 1000
_WRAPPING_OVERFLOW = (
    "Maximum element wrapping depth reached; there's "
    "probably a circularity in sqlalchemy that "
    "sqlapagination doesn't know how to handle."
)


def parse_order_by_clause(selectable: Union[Select, Query]) -> List["OrderByColumnWrapper"]:
    """Parse the ORDER BY clause of a selectable into a list of :class:`OC` instances."""
    return [
        OrderByColumnWrapper(clause)
        for clause in _flatten_order_by_clauses(get_order_by_clauses(selectable))
    ]


def _flatten_order_by_clauses(order_by_clauses: Sequence[Union[ClauseList, Column]]) -> List[Any]:
    """
    Flatten a list of :class:`sqlalchemy.sql.expression.ClauseList` instances
    into a list of :class:`sqlalchemy.sql.expression.ColumnElement` instances.
    """
    if isinstance(order_by_clauses, ClauseList):
        for subclause in order_by_clauses.clauses:
            for x in _flatten_order_by_clauses(subclause):
                yield x
    elif isinstance(order_by_clauses, (tuple, list)):
        for xs in order_by_clauses:
            for x in _flatten_order_by_clauses(xs):
                yield x
    else:
        yield order_by_clauses


class OrderByColumnWrapper:
    """Wrapper class for ordering columns; i.e.  instances of
    :class:`sqlalchemy.sql.expression.ColumnElement` appearing in the ORDER BY
    clause of a query we are paging."""

    def __init__(self, column_name_or_obj: Union[str, Any]) -> None:
        if isinstance(column_name_or_obj, str):
            column_name_or_obj = column(column_name_or_obj)

        if _get_order_direction(column_name_or_obj) is None:
            column_name_or_obj = asc(column_name_or_obj)

        self.column_name_or_obj = column_name_or_obj

        warn_if_column_nullable(self.comparable_value)

        self.full_name = str(self.element)
        try:
            table_name, name = self.full_name.split(".", 1)
        except ValueError:
            table_name = None
            name = self.full_name

        self.table_name = table_name
        self.name = name

    @property
    def quoted_full_name(self) -> str:
        return str(self).split()[0]

    @property
    def element(self) -> ColumnElement:
        """The ordering column/SQL expression with ordering modifier removed."""
        return _remove_order_direction(self.column_name_or_obj)

    @property
    def comparable_value(self) -> ClauseElement:
        """The ordering column/SQL expression in a form that is suitable for
        incorporating in a ``ROW(...) > ROW(...)`` comparision; i.e. with ordering
        modifiers and labels removed."""
        return strip_labels(self.element)

    @property
    def is_ascending(self) -> bool:
        """Returns ``True`` if this column is ascending, ``False`` if
        descending."""
        d = _get_order_direction(self.column_name_or_obj)
        if d is None:
            raise ValueError  # pragma: no cover
        return d == asc_op

    @property
    def reversed(self) -> "OrderByColumnWrapper":
        """An :class:`OC` representing the same column ordering, but reversed."""
        new_uo = _reverse_order_direction(self.column_name_or_obj)
        if new_uo is None:
            raise ValueError  # pragma: no cover
        return OrderByColumnWrapper(new_uo)

    def pair_for_comparison(self, value: Any, dialect: Any) -> Tuple[Any, Any]:
        """Return a pair of SQL expressions representing comparable values for
        this ordering column and a specified value.

        :param value: A value to compare this column against.
        :param dialect: The :class:`sqlalchemy.engine.interfaces.Dialect` in
            use.
        :returns: A pair `(a, b)` such that the comparison `a < b` is the
            condition for the value of this OC being past `value` in the paging
            order."""
        compval = self.comparable_value
        # If this OC is a column with a custom type, apply the custom
        # preprocessing to the comparsion value:
        try:
            value = compval.type.bind_processor(dialect)(value)
        except (TypeError, AttributeError):
            pass
        if self.is_ascending:
            return compval, value

        return value, compval

    def __str__(self) -> str:
        return str(self.column_name_or_obj)

    def __repr__(self) -> str:
        return "<OrderColumnWrapper: {}>".format(str(self))


def strip_labels(el: Union[Label, ColumnElement]) -> ClauseElement:
    """Remove labels from a
    :class:`sqlalchemy.sql.expression.ColumnElement`."""
    while isinstance(el, _LABELLED):
        try:
            el = el.element
        except AttributeError:
            raise ValueError  # pragma: no cover
    return el


def _get_order_direction(column_element: Any) -> Optional[Callable[[Any], Any]]:
    if column_element is None:
        return None

    modifier = getattr(column_element, "modifier", None)

    if modifier in {asc_op, desc_op}:
        return modifier

    return _get_order_direction(getattr(column_element, "element", None))


def _reverse_order_direction(column_element: ColumnElement) -> ColumnElement:
    """
    Given a :class:`sqlalchemy.sql.expression.ColumnElement`, return a copy
    with its ordering direction (ASC or DESC) reversed (if it has one).

    :param column_element: a :class:`sqlalchemy.sql.expression.ColumnElement`
    """
    x = copied = column_element._clone()
    for _ in range(_WRAPPING_DEPTH):
        mod = getattr(x, "modifier", None)
        if mod in (asc_op, desc_op):
            if mod == asc_op:
                x.modifier = desc_op
            else:
                x.modifier = asc_op
            return copied
        else:
            if not hasattr(x, "element"):
                return copied
            # Since we're going to change something inside x.element, we
            # need to clone another level deeper.
            x._copy_internals()
            x = x.element
    raise Exception(_WRAPPING_OVERFLOW)  # pragma: no cover


def _remove_order_direction(column_element: ColumnElement) -> ColumnElement:
    """
    Given a :class:`sqlalchemy.sql.expression.ColumnElement`, return a copy
    with its ordering modifiers (ASC/DESC, NULLS FIRST/LAST) removed (if it has
    any).

    :param column_element: a :class:`sqlalchemy.sql.expression.ColumnElement`
    """
    x = copied = column_element._clone()
    parent = None
    for _ in range(_WRAPPING_DEPTH):
        mod = getattr(x, "modifier", None)
        if mod in _UNSUPPORTED_ORDER_MODIFIERS:
            warn(
                "One of your order columns had a NULLS FIRST or NULLS LAST "
                "modifier; but sqlapagination does not support order columns "
                "with nulls. YOUR RESULTS WILL BE WRONG. See the "
                "Limitations section of the sqlakeyset README.md for more "
                "information."
            )
        if mod in _ORDER_MODIFIERS:
            x._copy_internals()
            if parent is None:
                # The modifier was at the top level; so just take the child.
                copied = x = x.element
            else:
                # Remove this link from the wrapping element chain and return
                # the top-level expression.
                parent.element = x = x.element
        else:
            if not hasattr(x, "element"):
                return copied
            parent = x
            # Since we might change something inside x.element, we
            # need to clone another level deeper.
            x._copy_internals()
            x = x.element
    raise Exception(_WRAPPING_OVERFLOW)  # pragma: no cover


class MappedOrderColumn(abc.ABC):
    """An ordering column in the context of a particular query/select.

    This wraps an :class:`OrderColumnWrapper` with one extra piece of information: how to
    retrieve the value of the ordering key from a result row. For some queries,
    this requires adding extra entities to the query; in this case,
    ``extra_column`` will be set."""

    def __init__(self, order_by_column_wrapper: OrderByColumnWrapper):
        self.order_by_wrapper = order_by_column_wrapper
        self.extra_column = None
        """An extra SQLAlchemy ORM entity that this ordering column needs to
        add to its query in order to retrieve its value at each row. If no
        extra data is required, the value of this property will be ``None``."""

    def get_from_row(self, internal_row: Any) -> Any:
        """Extract the value of this ordering column from a result row."""
        raise NotImplementedError  # pragma: no cover

    @property
    def order_by_clause(self):
        """The original ORDER BY (sub)clause underlying this column."""
        return self.order_by_wrapper.column_name_or_obj

    @property
    def reversed(self) -> "MappedOrderColumn":
        """A :class:`MappedOrderColumn` representing the same column in the
        reversed order."""
        column = copy(self)
        column.order_by_wrapper = column.order_by_wrapper.reversed
        return column

    def __str__(self) -> str:
        return str(self.order_by_wrapper)


class DirectColumn(MappedOrderColumn):
    """An ordering key that was directly included as a column in the original
    query."""

    def __init__(self, order_by_column_wrapper: OrderByColumnWrapper, index: int):
        super().__init__(order_by_column_wrapper)
        self.index = index

    def get_from_row(self, row):
        return row[self.index]

    def __repr__(self) -> str:
        return "Direct({}, {!r})".format(self.index, self.order_by_wrapper)


class AttributeColumn(MappedOrderColumn):
    """An ordering key that was included as a column attribute in the original
    query."""

    def __init__(
            self,
            order_by_column_wrapper: OrderByColumnWrapper,
            index: int,
            attr: Any
    ) -> None:
        super().__init__(order_by_column_wrapper)
        self.index = index
        self.attr = attr

    def get_from_row(self, row: Any) -> Any:
        if isinstance(row, Row):
            return getattr(row[self.index], self.attr)

        return getattr(row, self.attr)

    def __repr__(self) -> str:
        return "Attribute({}.{}, {!r})".format(self.index, self.attr, self.order_by_wrapper)


class AppendedColumn(MappedOrderColumn):
    """An ordering key that requires an additional column to be added to the
    original query."""

    _counter = 0

    def __init__(self, order_by_column_wrapper, name=None):
        super().__init__(order_by_column_wrapper)
        if not name:
            AppendedColumn._counter += 1
            name = "{}{}".format(ORDER_COL_PREFIX, AppendedColumn._counter)
        self.name = name
        self.extra_column = self.order_by_wrapper.comparable_value.label(self.name)

    def get_from_row(self, row: Any) -> Any:
        return getattr(row, self.name)

    @property
    def order_by_clause(self):
        col = self.extra_column
        return col if self.order_by_wrapper.is_ascending else col.desc()

    def __repr__(self) -> str:
        return "Appended({!r})".format(self.order_by_wrapper)


def find_order_key(
        order_column_wrapper: OrderByColumnWrapper,
        column_descriptions: List[Column]
) -> MappedOrderColumn:
    """Return a :class:`MappedOrderColumn` describing how to populate the
    ordering column `order_column_wrapper` from a query returning columns described by
    `column_descriptions`.

    :param order_column_wrapper: The :class:`OC` to look up.
    :param column_descriptions: The list of columns from which to attempt to
        derive the value of `order_column_wrapper`.
    :returns: A :class:`MappedOrderColumn`."""
    for index, desc in enumerate(column_descriptions):
        order_key = derive_order_key(order_column_wrapper, desc, index)
        if order_key is not None:
            return order_key

    # Couldn't find an existing column in the query from which we can
    # determine this ordering column; so we need to add one.
    return AppendedColumn(order_column_wrapper)


def derive_order_key(order_column_wrapper: OrderByColumnWrapper, desc: Any, column_index: int):
    """Attempt to derive the value of `order_column_wrapper` from a query column.

    :param order_column_wrapper: The :class:`OrderByColumnWrapper` to look up.
    :param desc: Either a column description as in
        :attr:`sqlalchemy.orm.query.Query.column_descriptions`, or a
        :class:`sqlalchemy.sql.expression.ColumnElement`.

    :returns: Either a :class:`MappedOrderColumn` or `None`."""
    if isinstance(desc, ColumnElement):
        if desc.compare(order_column_wrapper.comparable_value):
            return DirectColumn(order_column_wrapper, column_index)
        else:
            return None

    entity = desc["entity"]
    expr = desc["expr"]

    if isinstance(expr, Bundle):
        for key, col in dict(expr.columns).items():
            if strip_labels(col).compare(order_column_wrapper.comparable_value):
                return AttributeColumn(order_column_wrapper, column_index, key)

    try:
        is_a_table = bool(entity == expr)
    except (sqlalchemy.exc.ArgumentError, TypeError):
        is_a_table = False

    if isinstance(expr, Mapper) and expr.class_ == entity:
        is_a_table = True

    if is_a_table:
        mapper = class_mapper(desc["type"])
        try:
            prop = mapper.get_property_by_column(order_column_wrapper.element)
            return AttributeColumn(order_column_wrapper, column_index, prop.key)
        except sqlalchemy.orm.exc.UnmappedColumnError:
            pass

    # is an attribute of some kind
    if isinstance(expr, QueryableAttribute):
        try:
            mapper = expr.parent
            tname = mapper.local_table.description
            if order_column_wrapper.table_name == tname and order_column_wrapper.name == expr.name:
                return DirectColumn(order_column_wrapper, column_index)
        except AttributeError:
            pass

    try:
        if order_column_wrapper.quoted_full_name == OrderByColumnWrapper(expr).full_name:
            return DirectColumn(order_column_wrapper, column_index)
    except ArgumentError:
        pass
