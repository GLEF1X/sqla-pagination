from collections import OrderedDict
from typing import Any, Dict, TYPE_CHECKING, List

from sqlapagination.page import AbstractPage, T
from ...utils import get_value_from_row_by_column_name

if TYPE_CHECKING:
    from .paginator import ParsingMetadata


class KeySetPage(AbstractPage):

    def __init__(self, rows: List[T], parsing_metadata: "ParsingMetadata") -> None:
        super().__init__(rows)
        self._metadata = parsing_metadata
        self._rows = self._rows[:self._metadata.page_size]

        self._keyset_pairs_table = OrderedDict()
        for idx, row in enumerate(rows):
            self._keyset_pairs_table[idx] = {
                column.name: get_value_from_row_by_column_name(row, column.name)
                for column in self._metadata.order_by_columns
            }

        self._previous_bookmark = parsing_metadata.bookmark
        if self._previous_bookmark.get("direction", "forward") == "backward":
            self._keyset_pairs_table = OrderedDict(
                reversed(list(self._keyset_pairs_table.items()))
            )
            self._rows = list(reversed(self._rows))

        self._first_keyset_pair = self._keyset_pairs_table.get(0)
        self._last_keyset_pair = self._keyset_pairs_table.get(len(self._rows) - 1)

        # Additional keyset pair determines whether there are more entries in database
        self._additional_keyset_pair = self._keyset_pairs_table.get(len(self._rows))

        self._next_keyset_pair = self._additional_keyset_pair or self._last_keyset_pair
        self._previous_keyset_pair = self._previous_bookmark.get("keyset_pairs")

    @property
    def total_pages_count(self) -> int:
        raise TypeError("KeySetPage does not support total_pages_count")

    @property
    def current_page_number(self) -> int:
        raise TypeError(
            "KeySetPage does not support current_page_number property"
        )

    @property
    def last_page(self) -> Dict[str, Any]:
        raise TypeError(
            "KeySetPage does not support last_page property"
        )

    @property
    def first_page(self) -> Dict[str, Any]:
        return {}

    @property
    def is_full(self) -> bool:
        return len(self) == self._metadata.expected_entries_count

    @property
    def next(self) -> Dict[str, Any]:
        if not self.has_next:
            return {}

        return {
            "keyset_pairs": self._next_keyset_pair,
            "direction": "forward"
        }

    @property
    def has_next(self) -> bool:
        return self._next_keyset_pair is not None

    @property
    def previous(self) -> Dict[str, Any]:
        if not self.has_previous:
            return {}

        return {
            "keyset_pairs": self._previous_keyset_pair,
            "direction": "backward"
        }

    @property
    def has_previous(self) -> bool:
        return self._previous_keyset_pair is not None
