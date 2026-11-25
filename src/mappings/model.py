"""
Immutable building blocks of the mapping specification.

Vocabulary
----------
Global schema : `GlobalRelation` / `Attribute`, the target (mediated) schema.
Source side   : `SourceRelation` / `Column` / `RawFile`, a source file, how it is
                staged (cleanup, cast, rename) and the staged schema it exposes.
LAV mapping   : `LavMapping` / `ConjunctiveQuery` / `Atom` / terms / `Condition`,
                each source relation described as a view over the global schema.
Source links  : `SourceLink` / `LinkEnd`, how source relations connect to each other.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, NamedTuple

from mappings.base import (
    Aggregate,
    Containment,
    DataType,
    FileFormat,
    LinkKind,
    MappingError,
    Operator,
    as_str_tuple,
    as_tuple,
    check_unique,
    parse_enum,
    require_name,
    set_field,
)

STAGE_SUFFIX = "_stage"
STAGED_FORMAT = FileFormat.PARQUET
GLOBAL_FORMAT = FileFormat.PARQUET

Scalar = str | int | float


# Global schema
@dataclass(frozen=True)
class Attribute:
    name: str
    dtype: DataType
    description: str = ""

    def __post_init__(self) -> None:
        require_name(self.name, "Attribute")
        set_field(
            self, "dtype", parse_enum(DataType, self.dtype, f"Type de {self.name!r}")
        )


@dataclass(frozen=True)
class GlobalRelation:
    """A relation of the global schema, materialised as `<name>.parquet`."""

    name: str
    attributes: tuple[Attribute, ...]
    description: str = ""

    def __post_init__(self) -> None:
        require_name(self.name, "GlobalRelation")
        set_field(
            self,
            "attributes",
            as_tuple(self.attributes, Attribute, f"Attributes of {self.name!r}"),
        )

        if not self.attributes:
            raise MappingError(f"Global relation {self.name!r} without attributes")

        check_unique((a.name for a in self.attributes), f"Attributes of {self.name!r}")

    @property
    def attribute_names(self) -> tuple[str, ...]:
        return tuple(a.name for a in self.attributes)

    def attribute(self, name: str) -> Attribute:
        for attribute in self.attributes:
            if attribute.name == name:
                return attribute

        raise MappingError(f"Unknown attribute {name!r} in {self.name!r}")

    @property
    def filename(self) -> str:
        return f"{self.name}.{GLOBAL_FORMAT.value}"


# Source side : raw file -> staged file
class Replacement(NamedTuple):
    """A regex substitution (pattern, replacement), applied on text columns."""

    pattern: str
    replacement: str


def _replacement(pair: Any) -> Replacement:
    if isinstance(pair, Replacement):
        return pair

    if isinstance(pair, str) or not isinstance(pair, Sequence) or len(pair) != 2:
        raise MappingError(
            f"Invalid replacement (expected [pattern, remplacement]) : {pair!r}"
        )

    return Replacement(str(pair[0]), str(pair[1]))


@dataclass(frozen=True)
class RawFile:
    """Location and reading options of a raw source file."""

    path: str  # relative to the data directory (or absolute)
    format: FileFormat
    options: dict[str, Any] = field(default_factory=dict, hash=False)

    def __post_init__(self) -> None:
        require_name(self.path, "RawFile.path")
        set_field(self, "format", parse_enum(FileFormat, self.format, "Format"))
        set_field(self, "options", dict(self.options))


@dataclass(frozen=True)
class Column:
    """A column of the *staged* relation and how it is obtained from the raw file.

    Extraction rule, applied per column: `cleanup` -> `cast` -> rename.
      * `source`  : name in the raw file (None: same as `name`) -> rename.
      * `cast`    : the extraction forces `dtype` (otherwise `dtype` is the type
                    expected from the raw file and only used for validation).
      * `cleanup` : regex substitutions applied to text values before the cast.
    """

    name: str
    dtype: DataType
    source: str | None = None
    cast: bool = False
    cleanup: tuple[Replacement, ...] = ()
    description: str = ""

    def __post_init__(self) -> None:
        require_name(self.name, "Column")
        set_field(
            self,
            "dtype",
            parse_enum(DataType, self.dtype, f"Type of the column {self.name!r}"),
        )
        set_field(self, "cleanup", tuple(_replacement(p) for p in self.cleanup))
        if self.source == self.name:
            set_field(self, "source", None)
        if self.cleanup and not self.cast:
            raise MappingError(
                f"Column {self.name!r} : `cleanup` has sens only with `cast=True`"
            )

    @property
    def raw_name(self) -> str:
        return self.source or self.name


@dataclass(frozen=True)
class SourceRelation:
    """A source relation: one raw file, staged as `<stem>_stage.parquet`.

    `columns` lists the columns the pipeline relies on; other columns of the raw
    file go through staging untouched.
    """

    name: str
    source: str  # identifier of the data provider, e.g. "source1"
    raw: RawFile
    columns: tuple[Column, ...]
    stage_stem: str | None = None  # defaults to `name`
    description: str = ""

    def __post_init__(self) -> None:
        require_name(self.name, "SourceRelation")
        require_name(self.source, f"Source ideenfitier of {self.name!r}")
        set_field(
            self,
            "columns",
            as_tuple(self.columns, Column, f"Columns of {self.name!r}"),
        )

        if not self.columns:
            raise MappingError(f"Relation source {self.name!r} without column")

        check_unique((c.name for c in self.columns), f"Columns of {self.name!r}")
        check_unique(
            (c.raw_name for c in self.columns), f"Raw columns of {self.name!r}"
        )

        if self.stage_stem == self.name:
            set_field(self, "stage_stem", None)

    @property
    def staged_filename(self) -> str:
        return f"{self.stage_stem or self.name}{STAGE_SUFFIX}.{STAGED_FORMAT.value}"

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    def column(self, name: str) -> Column:
        for column in self.columns:
            if column.name == name:
                return column
        raise MappingError(f"Unknwon column {name!r} in {self.name!r}")

    @property
    def rename_map(self) -> dict[str, str]:
        """Raw name -> staged name, for renamed columns only."""
        return {c.raw_name: c.name for c in self.columns if c.raw_name != c.name}

    @property
    def casts(self) -> tuple[Column, ...]:
        """Columns whose type is forced during extraction."""
        return tuple(c for c in self.columns if c.cast)


# LAV mapping
@dataclass(frozen=True)
class Col:
    """Term: a column of the source relation (variable of the view head)."""

    name: str

    def __post_init__(self) -> None:
        require_name(self.name, "Col")


@dataclass(frozen=True)
class Const:
    """Term: a constant."""

    value: Scalar

    def __post_init__(self) -> None:
        if isinstance(self.value, bool) or not isinstance(
            self.value, (str, int, float)
        ):
            raise MappingError(f"Invalid constant : {self.value!r}")


@dataclass(frozen=True)
class Derived:
    """Term: a value computed from source columns, optionally aggregated.

    Used when a global attribute is not a source column but is computed from
    some of them, e.g. avg(NB_KW_Jour * 365). `expression` is opaque to this
    module; `inputs` lists the source columns it depends on (and is validated).
    """

    inputs: tuple[str, ...]
    expression: str
    aggregate: Aggregate | None = None

    def __post_init__(self) -> None:
        set_field(self, "inputs", as_str_tuple(self.inputs, "Derived.inputs"))
        require_name(self.expression, "Derived.expression")

        if self.aggregate is not None:
            set_field(
                self, "aggregate", parse_enum(Aggregate, self.aggregate, "Aggregation")
            )


Term = Col | Const | Derived
_TERM_TYPES = (Col, Const, Derived)


def as_term(term: Term | str) -> Term:
    """Return the term itself; a bare string is the shorthand for `Col(string)`."""
    return Col(term) if isinstance(term, str) else term


@dataclass(frozen=True)
class Atom:
    """An atom over a global relation: attribute -> term.

    Attributes that are not listed are existentially quantified (written `_`).
    A bare string is a shorthand for `Col(string)`.
    """

    relation: str
    terms: Mapping[str, Col | Const | Derived | str] = field(
        default_factory=dict, hash=False
    )

    def __post_init__(self) -> None:
        require_name(self.relation, "Atom")
        normalised: dict[str, Term] = {}

        for attribute, term in self.terms.items():
            term = as_term(term)
            normalised[attribute] = term

        set_field(self, "terms", normalised)


def _is_sequence(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes))


@dataclass(frozen=True)
class Condition:
    """A selection on a column of the source relation (restricts the view)."""

    column: str
    op: Operator
    value: Any  # scalar; sequence for `in`; [min, max] for `between`

    def __post_init__(self) -> None:
        require_name(self.column, "Condition")
        op = parse_enum(Operator, self.op, "Operator")

        set_field(self, "op", op)
        value = self.value

        if op is Operator.BETWEEN:
            if not _is_sequence(value) or len(value) != 2:
                raise MappingError(
                    f"Condition {self.column!r} : `between` expect [min, max]"
                )
            value = tuple(value)
        elif op is Operator.IN:
            if not _is_sequence(value) or not value:
                raise MappingError(
                    f"Condition {self.column!r} : `in` expect a non-empty list"
                )
            value = tuple(value)
        elif _is_sequence(value):
            raise MappingError(f"Condition {self.column!r} : expect a scalar value")

        set_field(self, "value", value)

    @property
    def values(self) -> tuple[Any, ...]:
        return (
            self.value if self.op in (Operator.IN, Operator.BETWEEN) else (self.value,)
        )


@dataclass(frozen=True)
class ConjunctiveQuery:
    """One conjunctive query over the global schema: atoms AND conditions."""

    atoms: tuple[Atom, ...]
    conditions: tuple[Condition, ...] = ()

    def __post_init__(self) -> None:
        set_field(self, "atoms", as_tuple(self.atoms, Atom, "Atomes"))
        set_field(
            self, "conditions", as_tuple(self.conditions, Condition, "Conditions")
        )

        if not self.atoms:
            raise MappingError("A query should contains at least one atome")


@dataclass(frozen=True)
class LavMapping:
    """LAV assertion for one source relation: `source ⊆ q1 U ... U qn`.

    Each `qi` is a conjunctive query over the global schema. With
    `Containment.EXACT` the inclusion becomes an equality.
    Source columns that appear in no atom are payload: they are not described
    by the global schema but remain available (e.g. to link sources).
    """

    source: str  # SourceRelation.name
    queries: tuple[ConjunctiveQuery, ...]
    containment: Containment = Containment.SOUND
    description: str = ""

    def __post_init__(self) -> None:
        require_name(self.source, "LavMapping")
        set_field(
            self,
            "queries",
            as_tuple(self.queries, ConjunctiveQuery, f"Queries of {self.source!r}"),
        )

        if not self.queries:
            raise MappingError(f"Mapping of {self.source!r} without query")

        set_field(
            self, "containment", parse_enum(Containment, self.containment, "Inclusion")
        )

    @property
    def global_relations(self) -> tuple[str, ...]:
        """Names of the global relations mentioned, in order of appearance."""
        seen: dict[str, None] = {}
        for query in self.queries:
            for atom in query.atoms:
                seen.setdefault(atom.relation)

        return tuple(seen)


# Links between source relations
@dataclass(frozen=True)
class LinkEnd:
    relation: str  # SourceRelation.name
    columns: tuple[str, ...]

    def __post_init__(self) -> None:
        require_name(self.relation, "LinkEnd")
        set_field(
            self,
            "columns",
            as_str_tuple(self.columns, f"Columns of {self.relation!r}"),
        )


@dataclass(frozen=True)
class SourceLink:
    """How two source relations connect (the join the transformations rely on)."""

    name: str
    left: LinkEnd
    right: LinkEnd
    kind: LinkKind
    description: str = ""

    def __post_init__(self) -> None:
        require_name(self.name, "SourceLink")
        set_field(self, "kind", parse_enum(LinkKind, self.kind, "Link type"))
        n_left, n_right = len(self.left.columns), len(self.right.columns)

        if self.kind is LinkKind.EQUALS and n_left != n_right:
            raise MappingError(
                f"Link {self.name!r} : `equals` expect as much columns on left and on right"
            )

        if self.kind is LinkKind.CONTAINS and (n_left, n_right) != (1, 1):
            raise MappingError(
                f"Link {self.name!r} : `contains` concerned in a single column on each side"
            )

        if self.kind is LinkKind.EXTRACTS and n_left != 1:
            raise MappingError(
                f"Link {self.name!r} : `extracts` starts from a single composite column on the left"
            )
