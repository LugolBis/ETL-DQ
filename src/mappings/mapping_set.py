"""`MappingSet`: the validated whole (global schema + sources + LAV mappings + links)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Any

from mappings import LinkKind
from mappings.base import (
    Containment,
    DataType,
    MappingError,
    Operator,
    as_tuple,
    check_unique,
    error_context,
    set_field,
)
from mappings.model import (
    Atom,
    Col,
    Condition,
    Const,
    Derived,
    GlobalRelation,
    LavMapping,
    LinkEnd,
    SourceLink,
    SourceRelation,
    as_term,
)


def _value_fits(dtype: DataType, value: Any) -> bool:
    if isinstance(value, bool):
        return False

    if dtype is DataType.BIGINT:
        return isinstance(value, int)

    if dtype is DataType.DOUBLE:
        return isinstance(value, (int, float))
    return isinstance(value, str)  # VARCHAR


@dataclass(frozen=True)
class MappingSet:
    """Everything needed to describe how sources feed the global schema.

    The order of `source_relations` is meaningful: it is the order in which
    `sources_for_global` / `dependencies` return the staged files, so it can
    be used directly as the positional arguments of a transformation.
    Paths are resolved against `data_dir`.
    """

    global_relations: tuple[GlobalRelation, ...]
    source_relations: tuple[SourceRelation, ...]
    mappings: tuple[LavMapping, ...]
    links: tuple[SourceLink, ...] = ()
    data_dir: Path = Path(".")

    def __post_init__(self) -> None:
        set_field(
            self,
            "global_relations",
            as_tuple(self.global_relations, GlobalRelation, "global_relations"),
        )
        set_field(
            self,
            "source_relations",
            as_tuple(self.source_relations, SourceRelation, "source_relations"),
        )
        set_field(self, "mappings", as_tuple(self.mappings, LavMapping, "mappings"))
        set_field(self, "links", as_tuple(self.links, SourceLink, "links"))
        set_field(self, "data_dir", Path(self.data_dir))
        self._validate()

    # Construction / serialisation (see `serialization.py`)
    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], data_dir: str | Path | None = None
    ) -> MappingSet:
        from .serialization import from_dict

        return from_dict(data, data_dir)

    @classmethod
    def from_json(
        cls, path: str | Path, data_dir: str | Path | None = None
    ) -> MappingSet:
        from mappings.serialization import load_json

        return load_json(path, data_dir)

    def to_dict(self) -> dict[str, Any]:
        from mappings.serialization import to_dict

        return to_dict(self)

    def to_json(self, path: str | Path) -> None:
        from mappings.serialization import dump_json

        dump_json(self, path)

    # Lookups
    @cached_property
    def _globals(self) -> dict[str, GlobalRelation]:
        return {g.name: g for g in self.global_relations}

    @cached_property
    def _sources(self) -> dict[str, SourceRelation]:
        return {s.name: s for s in self.source_relations}

    def global_relation(self, name: str) -> GlobalRelation:
        try:
            return self._globals[name]
        except KeyError:
            raise MappingError(f"Unknown global relation : {name!r}") from None

    def source_relation(self, name: str) -> SourceRelation:
        try:
            return self._sources[name]
        except KeyError:
            raise MappingError(f"Unknwon source relation : {name!r}") from None

    def source_ids(self) -> tuple[str, ...]:
        """Identifiers of the data providers (e.g. 'source1'), in declaration order."""
        return tuple(dict.fromkeys(s.source for s in self.source_relations))

    def relations_of_source(self, source_id: str) -> tuple[SourceRelation, ...]:
        return tuple(s for s in self.source_relations if s.source == source_id)

    # Mappings
    def mapping_for_source(self, name: str) -> LavMapping | None:
        self.source_relation(name)
        return next((m for m in self.mappings if m.source == name), None)

    def mappings_for_global(self, name: str) -> tuple[LavMapping, ...]:
        self.global_relation(name)
        return tuple(m for m in self.mappings if name in m.global_relations)

    def globals_for_source(self, name: str) -> tuple[GlobalRelation, ...]:
        mapping = self.mapping_for_source(name)
        if mapping is None:
            return ()
        return tuple(self._globals[g] for g in mapping.global_relations)

    def sources_for_global(self, name: str) -> tuple[SourceRelation, ...]:
        """Source relations whose view mentions the global relation (declaration order)."""
        wanted = {m.source for m in self.mappings_for_global(name)}
        return tuple(s for s in self.source_relations if s.name in wanted)

    def unmapped_sources(self) -> tuple[SourceRelation, ...]:
        mapped = {m.source for m in self.mappings}
        return tuple(s for s in self.source_relations if s.name not in mapped)

    def measures(self, global_relation: str) -> dict[str, Derived]:
        """Global attributes that are computed rather than copied, as stated by the views.

        Returns {attribute: Derived} (e.g. avg(NB_KW_Jour * 365)). Raises if two
        views define the same attribute differently.
        """
        self.global_relation(global_relation)
        found: dict[str, Derived] = {}

        for mapping in self.mappings:
            for query in mapping.queries:
                for atom in query.atoms:
                    if atom.relation != global_relation:
                        continue

                    for attribute, raw_term in atom.terms.items():
                        term = as_term(raw_term)

                        if (
                            isinstance(term, Derived)
                            and found.setdefault(attribute, term) != term
                        ):
                            raise MappingError(
                                f"{global_relation}.{attribute} : computed definitions "
                                "contradictory views"
                            )
        return found

    def link(self, name: str, kind: LinkKind | None = None) -> SourceLink:
        """A link by name; with `kind`, fail if the link is not of that kind."""
        for candidate in self.links:
            if candidate.name == name:
                if kind is not None and candidate.kind is not kind:
                    raise MappingError(
                        f"Link {name!r} : type {candidate.kind.value!r} "
                        f"instead of expected {kind.value!r}"
                    )
                return candidate

        raise MappingError(f"Unknown link : {name!r}")

    def links_of(self, relation: str) -> tuple[SourceLink, ...]:
        self.source_relation(relation)
        return tuple(
            lk for lk in self.links if relation in (lk.left.relation, lk.right.relation)
        )

    # Files
    def raw_path(self, source_relation: str) -> Path:
        return self.data_dir / self.source_relation(source_relation).raw.path

    def staged_path(self, source_relation: str) -> Path:
        return self.data_dir / self.source_relation(source_relation).staged_filename

    def global_path(self, global_relation: str) -> Path:
        return self.data_dir / self.global_relation(global_relation).filename

    def dependencies(self, global_relation: str) -> dict[str, tuple[Path, ...]]:
        """Staged files needed to build a global relation, grouped by provider.

        Same shape as the `sources_deps` hard-coded in the DAGs:
        {"source1": (staged files...), "source3": (...)}.
        """
        deps: dict[str, list[Path]] = {}
        for relation in self.sources_for_global(global_relation):
            deps.setdefault(relation.source, []).append(self.staged_path(relation.name))

        return {source: tuple(paths) for source, paths in deps.items()}

    # Validation
    def _validate(self) -> None:
        check_unique((g.name for g in self.global_relations), "Relations globales")
        check_unique((s.name for s in self.source_relations), "Relations sources")
        check_unique((s.staged_filename for s in self.source_relations), "Files staged")
        check_unique((lk.name for lk in self.links), "Links")
        check_unique(
            (m.source for m in self.mappings),
            "Mappings LAV (one per source: express the union within `queries`)",
        )

        for mapping in self.mappings:
            self._validate_mapping(mapping)

        for link in self.links:
            with error_context(f"Lien {link.name!r}"):
                self._validate_link_end(link.left)
                self._validate_link_end(link.right)

    def _validate_mapping(self, mapping: LavMapping) -> None:
        source = self.source_relation(mapping.source)
        for index, query in enumerate(mapping.queries):
            with error_context(f"Mapping of {source.name!r}, query {index}"):
                for atom in query.atoms:
                    self._validate_atom(source, atom)

                for condition in query.conditions:
                    self._validate_condition(source, condition)

    def _validate_atom(self, source: SourceRelation, atom: Atom) -> None:
        relation = self.global_relation(atom.relation)

        for attribute_name, term in atom.terms.items():
            attribute = relation.attribute(attribute_name)
            where = f"{relation.name}.{attribute.name}"

            if isinstance(term, Col):
                column = source.column(term.name)

                if column.dtype is not attribute.dtype:
                    raise MappingError(
                        f"{where} is {attribute.dtype.value} but "
                        f"{source.name}.{column.name} is {column.dtype.value}"
                    )
            elif isinstance(term, Const):
                if not _value_fits(attribute.dtype, term.value):
                    raise MappingError(
                        f"{where} is {attribute.dtype.value}, constant {term.value!r} incompatible"
                    )
            elif isinstance(term, Derived):
                for name in term.inputs:
                    source.column(name)

    @staticmethod
    def _validate_condition(source: SourceRelation, condition: Condition) -> None:
        column = source.column(condition.column)
        for value in condition.values:
            if not _value_fits(column.dtype, value):
                raise MappingError(
                    f"Condition on {source.name}.{column.name} ({column.dtype.value}) : "
                    f"value {value!r} incompatible"
                )

    def _validate_link_end(self, end: LinkEnd) -> None:
        relation = self.source_relation(end.relation)
        for name in end.columns:
            relation.column(name)

    # Human-readable rendering (Datalog-like notation)
    def describe_mapping(self, mapping: LavMapping) -> str:
        source = self.source_relation(mapping.source)
        head = f"{source.name}({', '.join(source.column_names)})"
        symbol = "⊆" if mapping.containment is Containment.SOUND else "="
        bodies = []

        for query in mapping.queries:
            parts = [self._render_atom(a) for a in query.atoms]
            parts += [_render_condition(c) for c in query.conditions]
            bodies.append(", ".join(parts))

        return f"{head}\n    {symbol} " + "\n    U ".join(bodies)

    def describe_link(self, link: SourceLink) -> str:
        return f"{_render_end(link.left)} {link.kind.value} {_render_end(link.right)}"

    def describe(self) -> str:
        """All LAV assertions, then the links between sources."""
        blocks = [self.describe_mapping(m) for m in self.mappings]

        if self.links:
            lines = ["-- Liens entre sources"]
            lines += [self.describe_link(lk) for lk in self.links]
            blocks.append("\n".join(lines))

        return "\n\n".join(blocks)

    def _render_atom(self, atom: Atom) -> str:
        relation = self.global_relation(atom.relation)
        args = [_render_term(_term_of(atom, a.name)) for a in relation.attributes]

        return f"{relation.name}({', '.join(args)})"


def _term_of(atom: Atom, attribute: str) -> Col | Const | Derived | None:
    term = atom.terms.get(attribute)
    return None if term is None else as_term(term)


def _render_term(term: Col | Const | Derived | None) -> str:
    if term is None:
        return "_"

    if isinstance(term, Col):
        return term.name

    if isinstance(term, Const):
        return repr(term.value)

    if term.aggregate is None:
        return term.expression

    return f"{term.aggregate.value}({term.expression})"


def _render_condition(condition: Condition) -> str:
    if condition.op is Operator.BETWEEN:
        low, high = condition.value
        return f"{condition.column} between {low!r} and {high!r}"

    if condition.op is Operator.IN:
        values = ", ".join(repr(v) for v in condition.value)
        return f"{condition.column} in ({values})"

    return f"{condition.column} {condition.op.value} {condition.value!r}"


def _render_end(end: LinkEnd) -> str:
    if len(end.columns) == 1:
        return f"{end.relation}.{end.columns[0]}"

    return f"{end.relation}.({', '.join(end.columns)})"
