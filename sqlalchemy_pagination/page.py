import abc
from typing import Sequence, TypeVar, Generic, overload, List, Iterator, Dict, Any

T = TypeVar('T')
_T_co = TypeVar('_T_co', covariant=True)


class AbstractPage(Sequence[T], Generic[T], abc.ABC):

    def __init__(self, rows: List[T]) -> None:
        self._rows = rows

    @property
    @abc.abstractmethod
    def last_page(self) -> Dict[str, Any]:
        pass

    @property
    @abc.abstractmethod
    def total_pages_count(self) -> int:
        pass

    @property
    @abc.abstractmethod
    def current_page_number(self) -> int:
        pass

    @property
    @abc.abstractmethod
    def is_full(self) -> bool:
        pass

    @property
    @abc.abstractmethod
    def has_next(self) -> bool:
        pass

    @property
    @abc.abstractmethod
    def has_previous(self) -> bool:
        pass

    @property
    @abc.abstractmethod
    def next(self) -> Dict[str, Any]:
        pass

    @property
    @abc.abstractmethod
    def previous(self) -> Dict[str, Any]:
        pass

    @property
    @abc.abstractmethod
    def first_page(self) -> Dict[str, Any]:
        pass

    @overload
    def __getitem__(self, i: int) -> _T_co:
        ...

    @overload
    def __getitem__(self, s: slice) -> Sequence[_T_co]:
        ...

    def __getitem__(self, i: int) -> _T_co:
        return self._rows[i]

    def __len__(self) -> int:
        return len(self._rows)

    def __iter__(self) -> Iterator[T]:
        return iter(self._rows)
