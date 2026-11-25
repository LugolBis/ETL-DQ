"""
Dict / JSON (de)serialisation of a `MappingSet`.

JSON layout (all keys except the first three are optional):

    {
      "data_dir": "/opt/airflow/.data",
      "global_schema": [{"name", "attributes": [{"name", "dtype", "description"}], ...}],
      "sources": [{"name", "source", "raw": {"path", "format", "options"},
                   "columns": [{"name", "dtype", "source", "cast", "cleanup"}], ...}],
      "mappings": [{"source", "containment",
                    "queries": [{"atoms": [{"relation", "terms": {attribute: term}}],
                                 "conditions": [{"column", "op", "value"}]}]}],
      "links": [{"name", "left": {"relation", "columns"}, "right": {...}, "kind"}]
    }

A term is a column name (string), {"const": value} or
{"derived": {"inputs": [...], "expression": "...", "aggregate": "avg"}}.
Unlisted attributes of an atom are existential. Unknown keys are rejected, and
errors carry the path of the faulty element (e.g. `mappings[3] : queries[0] : ...`).
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, TypeVar

from mappings.base import MappingError, error_context
from mappings.mapping_set import MappingSet
from mappings.model import (
    Atom,
    Attribute,
    Col,
    Column,
    Condition,
    ConjunctiveQuery,
    Const,
    Derived,
    GlobalRelation,
    LavMapping,
    LinkEnd,
    RawFile,
    SourceLink,
    SourceRelation,
    as_term,
)

T = TypeVar("T")


def _expect(
    data: Any, required: set[str], optional: frozenset[str] | set[str] = frozenset()
) -> Mapping[str, Any]:
    if not isinstance(data, Mapping):
        raise MappingError(f"object expected, received {type(data).__name__}")

    missing = required - data.keys()
    unknown = data.keys() - required - optional

    if missing:
        raise MappingError(f"missing key(s) : {sorted(missing)}")

    if unknown:
        raise MappingError(
            f"key(s) unknown : {sorted(unknown)} (valid keys : {sorted(required | optional)})"
        )

    return data


def _each[T](items: Any, label: str, parse: Callable[[Any], T]) -> tuple[T, ...]:
    if isinstance(items, (str, bytes)) or not isinstance(items, Sequence):
        raise MappingError(f"{label} : expected list")

    parsed: list[T] = []
    for index, item in enumerate(items):
        with error_context(f"{label}[{index}]"):
            parsed.append(parse(item))

    return tuple(parsed)


def _attribute(d: Any) -> Attribute:
    d = _expect(d, {"name", "dtype"}, {"description"})
    return Attribute(d["name"], d["dtype"], d.get("description", ""))


def _global_relation(d: Any) -> GlobalRelation:
    d = _expect(d, {"name", "attributes"}, {"description"})
    return GlobalRelation(
        d["name"],
        _each(d["attributes"], "attributes", _attribute),
        d.get("description", ""),
    )


def _column(d: Any) -> Column:
    d = _expect(d, {"name", "dtype"}, {"source", "cast", "cleanup", "description"})
    return Column(
        name=d["name"],
        dtype=d["dtype"],
        source=d.get("source"),
        cast=bool(d.get("cast", False)),
        cleanup=tuple(d.get("cleanup", ())),
        description=d.get("description", ""),
    )


def _raw_file(d: Any) -> RawFile:
    d = _expect(d, {"path", "format"}, {"options"})
    return RawFile(d["path"], d["format"], d.get("options", {}))


def _source_relation(d: Any) -> SourceRelation:
    d = _expect(
        d,
        {"name", "source", "raw", "columns"},
        {"stage_stem", "description"},
    )

    with error_context("raw"):
        raw = _raw_file(d["raw"])

    return SourceRelation(
        name=d["name"],
        source=d["source"],
        raw=raw,
        columns=_each(d["columns"], "columns", _column),
        stage_stem=d.get("stage_stem"),
        description=d.get("description", ""),
    )


def _term(value: Any) -> Col | Const | Derived:
    if isinstance(value, str):
        return Col(value)

    if isinstance(value, Mapping) and len(value) == 1:
        ((kind, payload),) = value.items()

        if kind == "col":
            return Col(payload)

        if kind == "const":
            return Const(payload)

        if kind == "derived":
            p = _expect(payload, {"inputs", "expression"}, {"aggregate"})
            return Derived(p["inputs"], p["expression"], p.get("aggregate"))

    raise MappingError(
        f"invalid term {value!r} (expected : column name, "
        '{"const": ...} or {"derived": {...}})'
    )


def _atom(d: Any) -> Atom:
    d = _expect(d, {"relation"}, {"terms"})
    terms = d.get("terms", {})

    if not isinstance(terms, Mapping):
        raise MappingError("terms : object {attribute: term} expected")

    parsed: dict[str, Col | Const | Derived] = {}
    for attribute, value in terms.items():
        with error_context(f"terms[{attribute!r}]"):
            parsed[attribute] = _term(value)

    return Atom(d["relation"], parsed)


def _condition(d: Any) -> Condition:
    d = _expect(d, {"column", "op", "value"})
    return Condition(d["column"], d["op"], d["value"])


def _query(d: Any) -> ConjunctiveQuery:
    d = _expect(d, {"atoms"}, {"conditions"})
    return ConjunctiveQuery(
        _each(d["atoms"], "atoms", _atom),
        _each(d.get("conditions", []), "conditions", _condition),
    )


def _mapping(d: Any) -> LavMapping:
    d = _expect(d, {"source", "queries"}, {"containment", "description"})
    return LavMapping(
        source=d["source"],
        queries=_each(d["queries"], "queries", _query),
        containment=d.get("containment", "sound"),
        description=d.get("description", ""),
    )


def _link_end(d: Any) -> LinkEnd:
    d = _expect(d, {"relation", "columns"})
    return LinkEnd(d["relation"], d["columns"])


def _link(d: Any) -> SourceLink:
    d = _expect(d, {"name", "left", "right", "kind"}, {"description"})

    with error_context("left"):
        left = _link_end(d["left"])

    with error_context("right"):
        right = _link_end(d["right"])

    return SourceLink(d["name"], left, right, d["kind"], d.get("description", ""))


def from_dict(
    data: Mapping[str, Any], data_dir: str | Path | None = None
) -> MappingSet:
    """
    Build a `MappingSet` from a plain dict (the JSON layout above).

    `data_dir` overrides the optional "data_dir" key of the config.
    """

    _expect(data, {"global_schema", "sources", "mappings"}, {"links", "data_dir"})
    directory = data_dir if data_dir is not None else data.get("data_dir", ".")

    return MappingSet(
        global_relations=_each(
            data["global_schema"], "global_schema", _global_relation
        ),
        source_relations=_each(data["sources"], "sources", _source_relation),
        mappings=_each(data["mappings"], "mappings", _mapping),
        links=_each(data.get("links", []), "links", _link),
        data_dir=Path(directory),
    )


def load_json(path: str | Path, data_dir: str | Path | None = None) -> MappingSet:
    with Path(path).open(encoding="utf-8") as handle:
        return from_dict(json.load(handle), data_dir)


def _with_description(d: dict[str, Any], description: str) -> dict[str, Any]:
    if description:
        d["description"] = description
    return d


def _term_to_json(term: Col | Const | Derived) -> Any:
    if isinstance(term, Col):
        return term.name

    if isinstance(term, Const):
        return {"const": term.value}

    payload: dict[str, Any] = {
        "inputs": list(term.inputs),
        "expression": term.expression,
    }

    if term.aggregate is not None:
        payload["aggregate"] = term.aggregate.value

    return {"derived": payload}


def _column_to_dict(c: Column) -> dict[str, Any]:
    d: dict[str, Any] = {"name": c.name, "dtype": c.dtype.value}
    if c.source:
        d["source"] = c.source

    if c.cast:
        d["cast"] = True

    if c.cleanup:
        d["cleanup"] = [list(r) for r in c.cleanup]

    return _with_description(d, c.description)


def _source_to_dict(s: SourceRelation) -> dict[str, Any]:
    raw: dict[str, Any] = {"path": s.raw.path, "format": s.raw.format.value}
    if s.raw.options:
        raw["options"] = dict(s.raw.options)

    d: dict[str, Any] = {"name": s.name, "source": s.source, "raw": raw}
    if s.stage_stem:
        d["stage_stem"] = s.stage_stem

    d["columns"] = [_column_to_dict(c) for c in s.columns]
    return _with_description(d, s.description)


def _query_to_dict(q: ConjunctiveQuery) -> dict[str, Any]:
    atoms = []
    for atom in q.atoms:
        entry: dict[str, Any] = {"relation": atom.relation}

        if atom.terms:
            entry["terms"] = {
                a: _term_to_json(as_term(t)) for a, t in atom.terms.items()
            }
        atoms.append(entry)

    d: dict[str, Any] = {"atoms": atoms}
    if q.conditions:
        d["conditions"] = [
            {
                "column": c.column,
                "op": c.op.value,
                "value": list(c.value) if isinstance(c.value, tuple) else c.value,
            }
            for c in q.conditions
        ]
    return d


def _mapping_to_dict(m: LavMapping) -> dict[str, Any]:
    d: dict[str, Any] = {
        "source": m.source,
        "containment": m.containment.value,
        "queries": [_query_to_dict(q) for q in m.queries],
    }
    return _with_description(d, m.description)


def _link_to_dict(lk: SourceLink) -> dict[str, Any]:
    d: dict[str, Any] = {
        "name": lk.name,
        "left": {"relation": lk.left.relation, "columns": list(lk.left.columns)},
        "right": {"relation": lk.right.relation, "columns": list(lk.right.columns)},
        "kind": lk.kind.value,
    }
    return _with_description(d, lk.description)


def to_dict(mapping_set: MappingSet) -> dict[str, Any]:
    data: dict[str, Any] = {
        "data_dir": str(mapping_set.data_dir),
        "global_schema": [
            _with_description(
                {
                    "name": g.name,
                    "attributes": [
                        _with_description(
                            {"name": a.name, "dtype": a.dtype.value}, a.description
                        )
                        for a in g.attributes
                    ],
                },
                g.description,
            )
            for g in mapping_set.global_relations
        ],
        "sources": [_source_to_dict(s) for s in mapping_set.source_relations],
        "mappings": [_mapping_to_dict(m) for m in mapping_set.mappings],
    }

    if mapping_set.links:
        data["links"] = [_link_to_dict(lk) for lk in mapping_set.links]

    return data


def dump_json(mapping_set: MappingSet, path: str | Path, indent: int = 2) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(to_dict(mapping_set), indent=indent, ensure_ascii=False)
    target.write_text(text, encoding="utf-8")
