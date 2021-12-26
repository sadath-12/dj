"""
Models for nodes.
"""

import os
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from sqlalchemy import String
from sqlalchemy.sql.schema import Column as SqlaColumn
from sqlmodel import Field, Relationship, SQLModel


def get_name_from_path(repository: Path, path: Path) -> str:
    """
    Compute the name of a node given its path and the repository path.
    """
    # strip anything before the repository
    path = path.relative_to(repository)

    # remove the "nodes" directory from the path
    path = path.relative_to(path.parts[0])

    # remove extension
    path = path.with_suffix("")

    # encode percent symbols and periods
    encoded = (
        str(path).replace("%", "%25").replace(".", "%2E").replace(os.path.sep, ".")
    )

    return encoded


class Config(SQLModel):
    """
    Configuration for a metric repository.
    """

    index: str


class Database(SQLModel, table=True):
    """
    A database.

    A simple example::

        name: druid
        description: An Apache Druid database
        URI: druid://localhost:8082/druid/v2/sql/
        read-only: true

    """

    id: Optional[int] = Field(default=None, primary_key=True)
    updated_at: datetime = Field(default_factory=partial(datetime.now, timezone.utc))
    name: str
    description: str = ""
    URI: str
    read_only: bool = True

    representations: List["Representation"] = Relationship(back_populates="database")


class Node(SQLModel, table=True):
    """
    A node.
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(sa_column=SqlaColumn("name", String, unique=True))
    description: str = ""

    created_at: datetime = Field(default_factory=partial(datetime.now, timezone.utc))
    updated_at: datetime = Field(default_factory=partial(datetime.now, timezone.utc))

    expression: Optional[str] = None

    # schema
    columns: List["Column"] = Relationship(back_populates="node")

    # storages
    representations: List["Representation"] = Relationship(back_populates="node")


class Representation(SQLModel, table=True):
    """
    A representation of data.

    Node nodes can have multiple representations of data, in different databases.
    """

    id: Optional[int] = Field(default=None, primary_key=True)

    node_id: int = Field(foreign_key="node.id")
    node: Node = Relationship(back_populates="representations")

    database_id: int = Field(foreign_key="database.id")
    database: Database = Relationship(back_populates="representations")
    catalog: Optional[str] = None
    schema_: Optional[str] = Field(None, alias="schema")
    table: str

    cost: float = 1.0

    # aggregation_level => for materialized metrics?


class Column(SQLModel, table=True):
    """
    A column.
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    type: str

    # only-on => for columns that are present in only a few DBs

    node_id: int = Field(foreign_key="node.id")
    node: Node = Relationship(back_populates="columns")
