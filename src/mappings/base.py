"""Shared exceptions, enumerations and small helpers of the mappings module."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from enum import Enum
from typing import Any, TypeVar

E = TypeVar("E", bound=Enum)
T = TypeVar("T")


class MappingError(ValueError):
    """Raised when a mapping specification is invalid or inconsistent."""


class DataType(str, Enum):
    """Logical column types (SQL-style names, as in the global schema)."""

    BIGINT = "BIGINT"
    DOUBLE = "DOUBLE"
    VARCHAR = "VARCHAR"


class FileFormat(str, Enum):
    CSV = "csv"
    PARQUET = "parquet"


class Containment(str, Enum):
    """How a source relation relates to its view over the global schema."""

    SOUND = "sound"  # source ⊆ view : the source may be incomplete (LAV default)
    EXACT = "exact"  # source = view


class Aggregate(str, Enum):
    AVG = "avg"
    SUM = "sum"
    MIN = "min"
    MAX = "max"
    COUNT = "count"


class Operator(str, Enum):
    EQ = "="
    NE = "!="
    LT = "<"
    LE = "<="
    GT = ">"
    GE = ">="
    IN = "in"
    BETWEEN = "between"


class LinkKind(str, Enum):
    """How two source relations are connected.

    EQUALS   : the columns are equal, pairwise (same arity on both sides).
    CONTAINS : the (single) left column contains the right one (tag/substring match).
    EXTRACTS : the right columns equal values extracted from the composite left column.
    """

    EQUALS = "equals"
    CONTAINS = "contains"
    EXTRACTS = "extracts"


def parse_enum[E: Enum](enum_cls: type[E], value: Any, what: str) -> E:
    """Convert `value` (member or case-insensitive string) to a member of `enum_cls`."""
    if isinstance(value, enum_cls):
        return value

    text = str(value)
    for member in enum_cls:
        if member.value == text or member.value.lower() == text.lower():
            return member

    allowed = ", ".join(repr(m.value) for m in enum_cls)
    raise MappingError(f"{what} invalid : {value!r} (possible values : {allowed})")


def set_field(obj: object, name: str, value: Any) -> None:
    """Assign a field of a frozen dataclass (used to normalise values in __post_init__)."""
    object.__setattr__(obj, name, value)


def require_name(value: Any, what: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise MappingError(f"{what} : empty name or invalid ({value!r})")


def check_unique(names: Iterable[str], what: str) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()

    for name in names:
        if name in seen:
            duplicates.add(name)
        seen.add(name)

    if duplicates:
        raise MappingError(f"{what} in duplicate : {sorted(duplicates)}")


def as_str_tuple(value: Any, what: str) -> tuple[str, ...]:
    """Normalise a non-empty sequence of names (a lone string is accepted)."""
    if isinstance(value, str):
        return (value,)

    try:
        items = tuple(value)
    except TypeError:
        raise MappingError(f"{what} : expected list, received {value!r}") from None

    if not items or not all(isinstance(i, str) and i for i in items):
        raise MappingError(
            f"{what} : expected non-empty list of names, received {value!r}"
        )
    return items


def as_tuple[T](items: Any, kind: type[T], what: str) -> tuple[T, ...]:
    """Normalise a sequence of `kind` instances (a lone instance is accepted)."""
    if isinstance(items, kind):
        return (items,)

    try:
        result = tuple(items)
    except TypeError:
        raise MappingError(f"{what} : list of {kind.__name__} expected") from None

    for item in result:
        if not isinstance(item, kind):
            raise MappingError(
                f"{what} : element of type {kind.__name__} expected, received {item!r}"
            )

    return result


@contextmanager
def error_context(where: str) -> Iterator[None]:
    """Prefix any MappingError raised inside the block with `where`."""
    try:
        yield
    except MappingError as exc:
        raise MappingError(f"{where} : {exc}") from exc
