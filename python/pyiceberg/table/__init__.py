# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import concurrent
import datetime
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
)

from pydantic import Field

from pyiceberg.expressions import (
    AlwaysTrue,
    And,
    BooleanExpression,
    visitors,
)
from pyiceberg.expressions.visitors import inclusive_projection
from pyiceberg.io import FileIO
from pyiceberg.io.pyarrow import project_table
from pyiceberg.manifest import (
    DataFile,
    ManifestContent,
    ManifestFile,
    files,
)
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.table.snapshots import Snapshot, SnapshotLogEntry
from pyiceberg.table.sorting import SortOrder
from pyiceberg.typedef import (
    EMPTY_DICT,
    Identifier,
    KeyDefaultDict,
    Properties,
)

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa
    from duckdb import DuckDBPyConnection


class Table:
    identifier: Identifier = Field()
    metadata: TableMetadata = Field()
    metadata_location: str = Field()
    io: FileIO

    def __init__(self, identifier: Identifier, metadata: TableMetadata, metadata_location: str, io: FileIO) -> None:
        self.identifier = identifier
        self.metadata = metadata
        self.metadata_location = metadata_location
        self.io = io

    def refresh(self) -> Table:
        """Refresh the current table metadata"""
        raise NotImplementedError("To be implemented")

    def name(self) -> Identifier:
        """Return the identifier of this table"""
        return self.identifier

    def scan(
        self,
        row_filter: Optional[BooleanExpression] = None,
        selected_fields: Tuple[str] = ("*",),
        case_sensitive: bool = True,
        snapshot_id: Optional[int] = None,
        options: Properties = EMPTY_DICT,
    ) -> TableScan[Any]:
        return DataScan(
            table=self,
            row_filter=row_filter or AlwaysTrue(),
            selected_fields=selected_fields,
            case_sensitive=case_sensitive,
            snapshot_id=snapshot_id,
            options=options,
        )

    def schema(self) -> Schema:
        """Return the schema for this table"""
        return next(schema for schema in self.metadata.schemas if schema.schema_id == self.metadata.current_schema_id)

    def schemas(self) -> Dict[int, Schema]:
        """Return a dict of the schema of this table"""
        return {schema.schema_id: schema for schema in self.metadata.schemas}

    def spec(self) -> PartitionSpec:
        """Return the partition spec of this table"""
        return next(spec for spec in self.metadata.partition_specs if spec.spec_id == self.metadata.default_spec_id)

    def specs(self) -> Dict[int, PartitionSpec]:
        """Return a dict the partition specs this table"""
        return {spec.spec_id: spec for spec in self.metadata.partition_specs}

    def sort_order(self) -> SortOrder:
        """Return the sort order of this table"""
        return next(
            sort_order for sort_order in self.metadata.sort_orders if sort_order.order_id == self.metadata.default_sort_order_id
        )

    def sort_orders(self) -> Dict[int, SortOrder]:
        """Return a dict of the sort orders of this table"""
        return {sort_order.order_id: sort_order for sort_order in self.metadata.sort_orders}

    def location(self) -> str:
        """Return the table's base location."""
        return self.metadata.location

    def current_snapshot(self) -> Optional[Snapshot]:
        """Get the current snapshot for this table, or None if there is no current snapshot."""
        if snapshot_id := self.metadata.current_snapshot_id:
            return self.snapshot_by_id(snapshot_id)
        return None

    def snapshot_by_id(self, snapshot_id: int) -> Optional[Snapshot]:
        """Get the snapshot of this table with the given id, or None if there is no matching snapshot."""
        try:
            return next(snapshot for snapshot in self.metadata.snapshots if snapshot.snapshot_id == snapshot_id)
        except StopIteration:
            return None

    def snapshot_by_name(self, name: str) -> Optional[Snapshot]:
        """Returns the snapshot referenced by the given name or null if no such reference exists."""
        if ref := self.metadata.refs.get(name):
            return self.snapshot_by_id(ref.snapshot_id)
        return None

    def history(self) -> List[SnapshotLogEntry]:
        """Get the snapshot history of this table."""
        return self.metadata.snapshot_log

    def __eq__(self, other: Any) -> bool:
        return (
            self.identifier == other.identifier
            and self.metadata == other.metadata
            and self.metadata_location == other.metadata_location
            if isinstance(other, Table)
            else False
        )


S = TypeVar("S", bound="TableScan", covariant=True)  # type: ignore


class TableScan(Generic[S], ABC):
    table: Table
    row_filter: BooleanExpression
    selected_fields: Tuple[str]
    case_sensitive: bool
    snapshot_id: Optional[int]
    options: Properties

    def __init__(
        self,
        table: Table,
        row_filter: Optional[BooleanExpression] = None,
        selected_fields: Tuple[str] = ("*",),
        case_sensitive: bool = True,
        snapshot_id: Optional[int] = None,
        options: Properties = EMPTY_DICT,
    ):
        self.table = table
        self.row_filter = row_filter or AlwaysTrue()
        self.selected_fields = selected_fields
        self.case_sensitive = case_sensitive
        self.snapshot_id = snapshot_id
        self.options = options

    def snapshot(self) -> Optional[Snapshot]:
        if self.snapshot_id:
            return self.table.snapshot_by_id(self.snapshot_id)
        return self.table.current_snapshot()

    def projection(self) -> Schema:
        snapshot_schema = self.table.schema()
        if snapshot := self.snapshot():
            if snapshot_schema_id := snapshot.schema_id:
                snapshot_schema = self.table.schemas()[snapshot_schema_id]

        if "*" in self.selected_fields:
            return snapshot_schema

        return snapshot_schema.select(*self.selected_fields, case_sensitive=self.case_sensitive)

    @abstractmethod
    def plan_files(self) -> Iterator[ScanTask]:
        ...

    @abstractmethod
    def to_arrow(self) -> pa.Table:
        ...

    @abstractmethod
    def to_pandas(self, **kwargs: Any) -> pd.DataFrame:
        ...

    def update(self: S, **overrides: Any) -> S:
        """Creates a copy of this table scan with updated fields."""
        return type(self)(**{**self.__dict__, **overrides})

    def use_ref(self, name: str) -> S:
        if self.snapshot_id:
            raise ValueError(f"Cannot override ref, already set snapshot id={self.snapshot_id}")
        if snapshot := self.table.snapshot_by_name(name):
            return self.update(snapshot_id=snapshot.snapshot_id)

        raise ValueError(f"Cannot scan unknown ref={name}")

    def select(self, *field_names: str) -> S:
        if "*" in self.selected_fields:
            return self.update(selected_fields=field_names)
        return self.update(selected_fields=tuple(set(self.selected_fields).intersection(set(field_names))))

    def filter(self, new_row_filter: BooleanExpression) -> S:
        return self.update(row_filter=And(self.row_filter, new_row_filter))

    def with_case_sensitive(self, case_sensitive: bool = True) -> S:
        return self.update(case_sensitive=case_sensitive)


class ScanTask(ABC):
    pass


@dataclass(init=False)
class FileScanTask(ScanTask):
    file: DataFile
    start: int
    length: int

    def __init__(self, data_file: DataFile, start: Optional[int] = None, length: Optional[int] = None):
        self.file = data_file
        self.start = start or 0
        self.length = length or data_file.file_size_in_bytes


def _check_content(file: DataFile) -> DataFile:
    try:
        if file.content == ManifestContent.DELETES:
            raise ValueError("PyIceberg does not support deletes: https://github.com/apache/iceberg/issues/6568")
        return file
    except AttributeError:
        # If the attribute is not there, it is a V1 record
        return file


class DataScan(TableScan["DataScan"]):
    def __init__(
        self,
        table: Table,
        row_filter: Optional[BooleanExpression] = None,
        selected_fields: Tuple[str] = ("*",),
        case_sensitive: bool = True,
        snapshot_id: Optional[int] = None,
        options: Properties = EMPTY_DICT,
    ):
        super().__init__(table, row_filter, selected_fields, case_sensitive, snapshot_id, options)

    def _build_partition_projection(self, spec_id: int) -> BooleanExpression:
        project = inclusive_projection(self.table.schema(), self.table.specs()[spec_id])
        return project(self.row_filter)

    @cached_property
    def partition_filters(self) -> KeyDefaultDict[int, BooleanExpression]:
        return KeyDefaultDict(self._build_partition_projection)

    def _build_manifest_evaluator(self, spec_id: int) -> Callable[[ManifestFile], bool]:
        spec = self.table.specs()[spec_id]
        return visitors.manifest_evaluator(spec, self.table.schema(), self.partition_filters[spec_id], self.case_sensitive)

    def _build_partition_evaluator(self, spec_id: int) -> Callable[[DataFile], bool]:
        spec = self.table.specs()[spec_id]
        partition_type = spec.partition_type(self.table.schema())
        partition_schema = Schema(*partition_type.fields)
        partition_expr = self.partition_filters[spec_id]

        evaluator = visitors.expression_evaluator(partition_schema, partition_expr, self.case_sensitive)
        return lambda data_file: evaluator(data_file.partition)

    def plan_files(self) -> Iterator[FileScanTask]:
        snapshot = self.snapshot()
        if not snapshot:
            return

        io = self.table.io

        # step 1: filter manifests using partition summaries
        # the filter depends on the partition spec used to write the manifest file, so create a cache of filters for each spec id

        manifest_evaluators: Dict[int, Callable[[ManifestFile], bool]] = KeyDefaultDict(self._build_manifest_evaluator)

        manifests = [
            manifest_file
            for manifest_file in snapshot.manifests(io)
            if manifest_evaluators[manifest_file.partition_spec_id](manifest_file)
        ]

        # step 2: filter the data files in each manifest
        # this filter depends on the partition spec used to write the manifest file

        partition_evaluators: Dict[int, Callable[[DataFile], bool]] = KeyDefaultDict(self._build_partition_evaluator)

        futures = []
        with ThreadPoolExecutor() as executor:
            for manifest in manifests:
                partition_filter = partition_evaluators[manifest.partition_spec_id]

                def read_manifest():
                    all_files = files(io.new_input(manifest.manifest_path))
                    return tuple(filter(partition_filter, tuple(all_files)))

                futures.append(executor.submit(read_manifest))

            tasks = []
            for future in concurrent.futures.as_completed(futures):
                matching_files = map(_check_content, future.result())
                tasks.extend(tuple(FileScanTask(file) for file in matching_files))

            return tasks

    def to_arrow(self) -> pa.Table:
        return project_table(
            self.plan_files(), self.table, self.row_filter, self.projection(), case_sensitive=self.case_sensitive
        )

    def to_pandas(self, **kwargs: Any) -> pd.DataFrame:
        return self.to_arrow().to_pandas(**kwargs)

    def to_duckdb(self, table_name: str, connection: Optional[DuckDBPyConnection] = None) -> DuckDBPyConnection:
        import duckdb

        con = connection or duckdb.connect(database=":memory:")
        con.register(table_name, self.to_arrow())

        return con
