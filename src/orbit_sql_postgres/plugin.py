# Copyright 2026-present Orbit Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Optional Orbit plugin wiring the PostgreSQL provider into Core's SQL capability."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from orbit import Application
from orbit.plugins import Plugin, PluginMetadata
from orbit_sql import SQL_DATABASE_KEY

from orbit_sql_postgres.config import PostgresConfig
from orbit_sql_postgres.database import PostgresDatabase


class PostgresPlugin(Plugin):
    """Register a lazily opened PostgreSQL pool as a Core-owned SQL resource.

    Setup only registers a resource factory. The pool is acquired on the first asynchronous
    container resolution and is closed by Core's container during application shutdown.
    """

    metadata = PluginMetadata(
        name="orbit-sql-postgres",
        version="0.1.0a1",
        capabilities=frozenset({"database.sql.postgres"}),
    )

    def __init__(self, config: PostgresConfig) -> None:
        """Capture validated settings without opening a connection or exposing the DSN."""
        if not isinstance(config, PostgresConfig):
            raise TypeError("PostgresPlugin requires PostgresConfig.")
        self._config = config

    def setup(self, application: Application) -> None:
        """Register the SQL adapter as a lazily acquired Core container resource."""
        if not isinstance(application, Application):
            raise TypeError("PostgresPlugin requires an Orbit Application.")
        application.container.register_resource(SQL_DATABASE_KEY, lambda _: self._database())

    @asynccontextmanager
    async def _database(self) -> AsyncIterator[PostgresDatabase]:
        """Open the pool on demand and always release it through Core resource cleanup."""
        database = await PostgresDatabase.open(self._config)
        try:
            yield database
        finally:
            await database.aclose()


__all__ = ["PostgresPlugin"]
