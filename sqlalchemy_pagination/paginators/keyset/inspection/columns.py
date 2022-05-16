from _warnings import warn
from typing import Any


def warn_if_column_nullable(column: Any) -> None:
    try:
        if column.nullable or column.property.columns[0].nullable:
            warn(
                "Ordering by nullable column {} can cause rows to be "
                "incorrectly omitted from the results. "
                "See the sqlalchemy_pagination README.md for more details.".format(column),
                stacklevel=7,
            )
    except (AttributeError, IndexError, KeyError):
        pass
