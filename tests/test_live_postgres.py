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
"""Opt-in contract checks against a real PostgreSQL server."""

from __future__ import annotations

import os
from contextlib import suppress
from urllib.parse import urlsplit
from uuid import uuid4

import pytest

from orbit_sql_postgres import PostgresConfig, PostgresDatabase


@pytest.mark.asyncio
async def test_live_postgres_queries_transaction_rollback_and_cleanup() -> None:
    """Verify query, bounded fetch, transaction, and pool cleanup against PostgreSQL."""
    dsn = os.environ.get("ORBIT_POSTGRES_DSN")
    if not dsn:
        pytest.skip("Set ORBIT_POSTGRES_DSN to run the live PostgreSQL integration check.")
    host = urlsplit(dsn).hostname
    local = host in {"localhost", "127.0.0.1", "::1"}
    if not local and os.environ.get("ORBIT_POSTGRES_ALLOW_REMOTE_TESTS") != "1":
        pytest.skip(
            "Set ORBIT_POSTGRES_ALLOW_REMOTE_TESTS=1 to target a non-local test database."
        )

    database = await PostgresDatabase.open(PostgresConfig(dsn=dsn, ssl=not local))
    table = f"orbit_live_{uuid4().hex}"
    quoted_table = f'"{table}"'
    try:
        await database.execute(
            f"CREATE TABLE {quoted_table} (id INTEGER PRIMARY KEY, value TEXT NOT NULL)"
        )
        inserted = await database.execute(
            f"INSERT INTO {quoted_table} (id, value) VALUES ($1, $2)", (1, "committed")
        )
        assert inserted.rowcount == 1

        with pytest.raises(RuntimeError, match="rollback sentinel"):
            async with database.transaction() as transaction:
                await transaction.execute(
                    f"INSERT INTO {quoted_table} (id, value) VALUES ($1, $2)",
                    (2, "rolled back"),
                )
                raise RuntimeError("rollback sentinel")

        row = await database.fetch_one(
            f"SELECT id, value FROM {quoted_table} WHERE id = $1", (1,)
        )
        rows = await database.fetch_all(
            f"SELECT id, value FROM {quoted_table} ORDER BY id", limit=10
        )
        rolled_back = await database.fetch_one(
            f"SELECT id FROM {quoted_table} WHERE id = $1", (2,)
        )
        assert row is not None and dict(row) == {"id": 1, "value": "committed"}
        assert tuple(dict(result) for result in rows) == ({"id": 1, "value": "committed"},)
        assert rolled_back is None
    finally:
        with suppress(Exception):
            await database.execute(f"DROP TABLE IF EXISTS {quoted_table}")
        await database.aclose()
