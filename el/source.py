"""Build a dlt resource for one source table according to its mode."""
from __future__ import annotations

from typing import Any, Iterator

from sqlalchemy import MetaData, Table, select
from sqlalchemy.engine import Engine

from dlt.sources.sql_database import sql_table

from el.settings import ChildConfig, SourceDefinition, TableConfig


def _norm(engine: Engine, name):
    """Normalize an identifier to the dialect's form expected by reflection.

    Oracle stores unquoted names in UPPERCASE but SQLAlchemy's reflection expects
    its normalized (lower-case) form, so passing "ZZ_MODEL_INFO" fails while
    "zz_model_info" works. This delegates to the dialect: a no-op for MSSQL,
    upper->lower folding for Oracle. Applied to schema, table and column names so
    users can write real (e.g. UPPERCASE) Oracle names in sources.yml.
    """
    if not name:
        return name
    if isinstance(name, (list, tuple)):
        return [engine.dialect.normalize_name(n) for n in name]
    return engine.dialect.normalize_name(name)


def _col(sa_table, name: str):
    """Case-insensitively resolve a column on a reflected table.

    Reflected column keys differ by dialect (Oracle folds to lower-case, MSSQL
    keeps the DB case), and `normalize_name` folds all-UPPERCASE names to
    lower-case even for MSSQL — so match case-insensitively against the actual
    reflected columns rather than assuming a normalized form.
    """
    cols = sa_table.c
    if name in cols:
        return cols[name]
    lname = name.lower()
    for c in cols:
        if c.name.lower() == lname:
            return c
    raise KeyError(f"column {name!r} not found on table {sa_table.name!r}")


def build_resource(
    engine: Engine,
    source: SourceDefinition,
    table: TableConfig,
    batch_value: Any = None,
):
    """Return a dlt resource configured for the table's load mode.

    - full_replace: read whole table, write_disposition="replace".
    - scd2:         read whole snapshot, merge/scd2 with natural key.
    - batch:        filter source by ``batch_column == batch_value``, append.
    """
    common = dict(
        credentials=engine,
        table=_norm(engine, table.name),
        schema=_norm(engine, source.schema),
    )

    if table.mode == "full_replace":
        return sql_table(**common, write_disposition="replace")

    if table.mode == "scd2":
        return sql_table(
            **common,
            write_disposition={"disposition": "merge", "strategy": "scd2"},
            merge_key=_norm(engine, table.scd_natural_key),
        )

    if table.mode == "batch":
        value = batch_value

        def query_adapter(query, sa_table):
            return query.where(_col(sa_table, table.batch_column) == value)

        return sql_table(
            **common,
            write_disposition="append",
            query_adapter_callback=query_adapter,
        )

    raise ValueError(f"Unknown mode: {table.mode}")


# --------------------------------------------------------------------------- #
# Batch parent/child (master-detail) tree
# --------------------------------------------------------------------------- #

class Node:
    """A table in an expanded batch tree.

    The root is the batch parent (``child_key``/``parent_key`` are None);
    every other node is a child related to ``parent`` by
    ``child_key`` (on this table) = ``parent_key`` (on the parent table).
    """

    __slots__ = ("table_name", "child_key", "parent_key", "parent", "path", "children")

    def __init__(self, table_name, child_key, parent_key, parent, path):
        self.table_name = table_name
        self.child_key = child_key
        self.parent_key = parent_key
        self.parent: "Node | None" = parent
        self.path = path
        self.children: list["Node"] = []


def build_batch_tree(table: TableConfig) -> Node:
    """Expand a batch TableConfig (with nested `children`) into a Node tree."""
    root = Node(table.name, None, None, None, table.name)

    def add(child_cfgs: list[ChildConfig], parent_node: Node) -> None:
        for c in child_cfgs:
            node = Node(c.name, c.child_key, c.parent_key, parent_node, f"{parent_node.path} > {c.name}")
            parent_node.children.append(node)
            add(c.children, node)

    add(table.children, root)
    return root


def iter_preorder(node: Node) -> Iterator[Node]:
    """Parents before children (load order)."""
    yield node
    for c in node.children:
        yield from iter_preorder(c)


def iter_postorder(node: Node) -> Iterator[Node]:
    """Children before parents (delete order)."""
    for c in node.children:
        yield from iter_postorder(c)
    yield node


class _SourceMembership:
    """Builds SQLAlchemy filters selecting the rows of a node in the batch.

    membership(root)  -> batch_column == value
    membership(child) -> child_key IN (SELECT parent_key FROM parent WHERE membership(parent))
    """

    def __init__(self, engine: Engine, schema: str, batch_column: str, value: Any):
        self.engine = engine
        self.schema = _norm(engine, schema)
        self.batch_column = batch_column
        self.value = value
        self._cache: dict[str, Table] = {}

    def _reflect(self, table_name: str) -> Table:
        name = _norm(self.engine, table_name)
        if name not in self._cache:
            self._cache[name] = Table(
                name, MetaData(), schema=self.schema, autoload_with=self.engine
            )
        return self._cache[name]

    def membership(self, node: Node):
        t = self._reflect(node.table_name)
        if node.parent is None:
            return _col(t, self.batch_column) == self.value
        parent_t = self._reflect(node.parent.table_name)
        subq = select(_col(parent_t, node.parent_key)).where(self.membership(node.parent))
        return _col(t, node.child_key).in_(subq)

    def child_where(self, node: Node, sa_table):
        """WHERE for extracting a child node, using dlt's reflected table for its own column."""
        parent_t = self._reflect(node.parent.table_name)
        subq = select(_col(parent_t, node.parent_key)).where(self.membership(node.parent))
        return _col(sa_table, node.child_key).in_(subq)


def build_child_resource(
    engine: Engine,
    source: SourceDefinition,
    node: Node,
    batch_column: str,
    value: Any,
):
    """Return an append resource for a child node, filtered by its batch lineage."""
    membership = _SourceMembership(engine, source.schema, batch_column, value)

    def query_adapter(query, sa_table):
        return query.where(membership.child_where(node, sa_table))

    return sql_table(
        credentials=engine,
        table=_norm(engine, node.table_name),
        schema=_norm(engine, source.schema),
        write_disposition="append",
        query_adapter_callback=query_adapter,
    )
