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
"""Adapter registration contract for the PostgreSQL implementation."""

from orbit_sql import SQLAdapter
from pydantic import BaseModel

from orbit_sql_postgres.config import PostgresConfig
from orbit_sql_postgres.database import PostgresDatabase


class PostgresAdapter:
    """Open PostgreSQL resources for registration in an ``orbit-sql`` registry."""

    @property
    def name(self) -> str:
        """Return the stable registry key used for PostgreSQL."""
        return "postgres"

    @property
    def configuration_model(self) -> type[BaseModel]:
        """Return the validated, secret-aware PostgreSQL configuration type."""
        return PostgresConfig

    async def connect(self, configuration: BaseModel) -> PostgresDatabase:
        """Open a bounded asyncpg pool from the explicitly supplied settings."""
        if not isinstance(configuration, PostgresConfig):
            raise TypeError("PostgresAdapter requires PostgresConfig.")
        return await PostgresDatabase.open(configuration)


assert isinstance(PostgresAdapter(), SQLAdapter)

__all__ = ["PostgresAdapter"]
