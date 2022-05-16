from typing import List, Any, Dict

from sqlalchemy_pagination.constants import DEFAULT_PAGE_SIZE
from sqlalchemy_pagination.page import AbstractPage, T


class LimitOffsetPage(AbstractPage):

    def __init__(
            self,
            rows: List[T],
            page_size: int = DEFAULT_PAGE_SIZE,
            offset: int = 0,
            rows_count: int = 0
    ) -> None:
        super().__init__(rows)
        self._page_size = page_size
        self._rows_count = rows_count
        self._current_offset = offset

    @property
    def total_pages_count(self) -> int:
        return self._rows_count // self._page_size

    @property
    def current_page_number(self) -> int:
        return self._current_offset // self._page_size

    @property
    def first_page(self) -> Dict[str, Any]:
        return {"offset": 0}

    @property
    def last_page(self) -> Dict[str, Any]:
        if self._rows_count - self._page_size < 0:
            return {}

        return {"offset": self._rows_count - self._page_size}

    @property
    def is_full(self) -> bool:
        return len(self) == self._page_size

    @property
    def next(self) -> Dict[str, Any]:
        if not self.has_next:
            return {}

        return {
            "offset": self._current_offset + self._page_size
        }

    @property
    def has_next(self) -> bool:
        return self._current_offset + self._page_size < self._rows_count

    @property
    def previous(self) -> Dict[str, Any]:
        if not self.has_previous:
            return {}

        return {
            "offset": self._current_offset - self._page_size
        }

    @property
    def has_previous(self) -> bool:
        if self._current_offset == 0 or self._current_offset < self._page_size:
            return False

        return self._current_offset < self._page_size
