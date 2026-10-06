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
"""Contract tests for the PostgreSQL adapter using an isolated fake asyncpg pool."""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import asyncpg
import pytest
from orbit import Application, ApplicationConfig
from orbit_data import Repository, UnitOfWork
from orbit_sql import (
    SQL_DATABASE_KEY,
    DatabaseError,
    SQLAdapterRegistry,
    SQLDatabase,
    SQLExecution,
    SQLRepository,
    SQLUnitOfWork,
)
from pydantic import BaseModel, SecretStr, ValidationError

from orbit_sql_postgres import PostgresAdapter, PostgresConfig, PostgresPlugin
from orbit_sql_postgres.database import PostgresDatabase


class _FakeTransaction:
    """Record transaction lifecycle calls without a database server."""

    def __init__(self, connection):
        self.connection = connection

    async def start(self):
        self.connection.started += 1

    async def commit(self):
        self.connection.committed += 1
        if self.connection.commit_error is not None:
            raise self.connection.commit_error

    async def rollback(self):
        self.connection.rolled_back += 1

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exception_type, _exception, _traceback):
        if exception_type is None:
            await self.commit()
        else:
            await self.rollback()
        return False


class _FakeCursor:
    """Return no more than the requested number of fake result records."""

    def __init__(self, records):
        self.records = records
        self.requested = 0

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        for record in self.records:
            self.requested += 1
            yield record


class _FakeConnection:
    """Provide the small asyncpg connection surface used by the adapter."""

    def __init__(self, records=()):
        self.records = list(records)
        self.started = 0
        self.committed = 0
        self.rolled_back = 0
        self.cursor_instance = None
        self.released = 0
        self.commit_error = None

    async def execute(self, statement, *parameters):
        self.statement = statement
        self.parameters = parameters
        return "UPDATE 3"

    async def fetchrow(self, statement, *parameters):
        self.statement = statement
        self.parameters = parameters
        return self.records[0] if self.records else None

    def cursor(self, statement, *parameters, prefetch):
        self.statement = statement
        self.parameters = parameters
        self.cursor_instance = _FakeCursor(self.records)
        return self.cursor_instance

    def transaction(self):
        return _FakeTransaction(self)


class _Acquire:
    """Async context manager for the fake pool's single connection."""

    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *_args):
        self.connection.released += 1
        return False


class _FakePool:
    """Pool fixture recording graceful close and forced termination."""

    def __init__(self, connection):
        self.connection = connection
        self.closed = 0
        self.terminated = 0

    def acquire(self):
        return _Acquire(self.connection)

    async def close(self):
        self.closed += 1

    def terminate(self):
        self.terminated += 1


@pytest.fixture
def fake_pool(monkeypatch):
    """Patch asyncpg pool creation and yield the resulting isolated fake pool."""
    connection = _FakeConnection(
        [
            {
                "amount": Decimal("12.50"),
                "created_at": datetime(2026, 10, 4, tzinfo=UTC),
                "id": UUID("12345678-1234-5678-1234-567812345678"),
            }
        ]
    )
    pool = _FakePool(connection)
    observed = {}

    async def create_pool(**kwargs):
        observed.update(kwargs)
        return pool

    monkeypatch.setattr(asyncpg, "create_pool", create_pool)
    return pool, observed


async def test_adapter_registers_and_maps_common_scalar_values(fake_pool) -> None:
    """The adapter returns Core SQL rows and uses the portable positional binding contract."""
    pool, observed = fake_pool
    registry = SQLAdapterRegistry()
    registry.register(PostgresAdapter())
    config = PostgresConfig(dsn="postgresql://user:secret@localhost/app")

    database = await registry.connect("postgres", config)
    try:
        result = await database.execute("UPDATE sample SET value = $1", ("x",))
        row = await database.fetch_one("SELECT $1", (1,))

        assert result.rowcount == 3
        assert row is not None
        assert row["amount"] == Decimal("12.50")
        assert row["id"] == UUID("12345678-1234-5678-1234-567812345678")
        assert pool.connection.statement == "SELECT $1"
        assert pool.connection.parameters == (1,)
        assert observed["ssl"] is True
        assert "secret" not in repr(config)
    finally:
        await database.aclose()


async def test_core_sql_repository_and_postgres_adapter_compose(fake_pool) -> None:
    """The provider leaf plugs into Core SQL and orbit-sql without a vendor-facing repository."""
    pool, _observed = fake_pool
    registry = SQLAdapterRegistry()
    registry.register(PostgresAdapter())
    database = await registry.connect(
        "postgres", PostgresConfig(dsn="postgresql://u:p@localhost/app")
    )

    class Note(BaseModel):
        id: UUID
        amount: Decimal
        created_at: datetime

    note = Note(
        id=UUID("12345678-1234-5678-1234-567812345678"),
        amount=Decimal("13.50"),
        created_at=datetime(2026, 10, 4, tzinfo=UTC),
    )
    repository = SQLRepository(database, Note, table="notes", primary_key="id")
    unit = SQLUnitOfWork(database, (repository,))
    assert isinstance(database, SQLDatabase)
    assert isinstance(repository, Repository)
    assert isinstance(unit, UnitOfWork)

    try:
        await repository.add(note)
        assert pool.connection.statement == (
            'INSERT INTO "notes" ("id", "amount", "created_at") VALUES ($1, $2, $3)'
        )
        assert pool.connection.parameters == (note.id, note.amount, note.created_at)
        assert await repository.get(note.id) == Note(
            id=UUID("12345678-1234-5678-1234-567812345678"),
            amount=Decimal("12.50"),
            created_at=datetime(2026, 10, 4, tzinfo=UTC),
        )

        async with unit as active:
            assert await active.repository(Note).update(note)
        assert pool.connection.committed == 1
        assert pool.connection.statement.startswith('UPDATE "notes" SET ')
        assert pool.connection.parameters == (note.amount, note.created_at, note.id)
    finally:
        await database.aclose()


async def test_postgres_plugin_registers_lazy_resource_and_core_closes_it(fake_pool) -> None:
    """Postgres plugin registration is lazy and Core owns the pool's final cleanup."""
    pool, observed = fake_pool
    application = Application(ApplicationConfig(name="postgres-plugin-test"))
    plugin = PostgresPlugin(PostgresConfig(dsn="postgresql://user:secret@localhost/app"))
    application.register_plugin(plugin)

    await application.startup()
    assert observed == {}

    database = await application.container.aresolve(SQL_DATABASE_KEY)
    assert isinstance(database, SQLDatabase)
    assert await database.execute("SELECT 1") == SQLExecution(rowcount=3)
    assert observed["ssl"] is True

    await application.stop()
    assert pool.closed == 1
    assert pool.terminated == 0


async def test_fetch_all_bounds_cursor_iteration_and_errors_on_excess(fake_pool) -> None:
    """PostgreSQL reads at most one sentinel row above the caller's limit."""
    _pool, _observed = fake_pool
    _pool.connection.records.append({"amount": Decimal("1.00")})
    database = await PostgresDatabase.open(PostgresConfig(dsn="postgresql://user:pass@db/app"))

    with pytest.raises(DatabaseError) as caught:
        await database.fetch_all("SELECT id FROM sample", limit=1)

    assert caught.value.code == "database.result-limit"
    assert _pool.connection.cursor_instance.requested == 2
    await database.aclose()


async def test_transaction_commits_and_rolls_back_user_exceptions(fake_pool) -> None:
    """Driver transactions preserve application exceptions while choosing commit or rollback."""
    pool, _observed = fake_pool
    database = await PostgresDatabase.open(PostgresConfig(dsn="postgresql://u:p@db/app"))

    async with database.transaction() as transaction:
        await transaction.execute("UPDATE sample SET value = $1", ("committed",))
    assert pool.connection.committed == 1

    with pytest.raises(ValueError, match="application error"):
        async with database.transaction():
            raise ValueError("application error")
    assert pool.connection.rolled_back == 1
    await database.aclose()


async def test_unique_violation_maps_to_provider_neutral_error(fake_pool) -> None:
    """The PostgreSQL adapter preserves conflict semantics without exposing driver details."""
    pool, _observed = fake_pool

    async def duplicate_execute(_statement, *_parameters):
        raise asyncpg.UniqueViolationError("private table and constraint details")

    pool.connection.execute = duplicate_execute
    database = await PostgresDatabase.open(PostgresConfig(dsn="postgresql://u:p@db/app"))
    try:
        with pytest.raises(DatabaseError) as caught:
            await database.execute("INSERT INTO sample VALUES ($1)", (1,))
        assert caught.value.code == "database.constraint-conflict"
        assert "private" not in str(caught.value)
    finally:
        await database.aclose()


async def test_deferred_unique_violation_at_commit_uses_conflict_code(fake_pool) -> None:
    """A deferred constraint failure at transaction exit keeps the same stable error code."""
    pool, _observed = fake_pool
    pool.connection.commit_error = asyncpg.UniqueViolationError("private deferred constraint")
    database = await PostgresDatabase.open(PostgresConfig(dsn="postgresql://u:p@db/app"))
    try:
        with pytest.raises(DatabaseError) as caught:
            async with database.transaction():
                pass
        assert caught.value.code == "database.constraint-conflict"
        assert "private" not in str(caught.value)
    finally:
        await database.aclose()


async def test_transaction_cancellation_rolls_back_and_releases_connection(fake_pool) -> None:
    """Cancellation rolls back the driver transaction and returns its connection to the pool."""
    pool, _observed = fake_pool
    database = await PostgresDatabase.open(PostgresConfig(dsn="postgresql://u:p@db/app"))
    entered = asyncio.Event()
    transactions = []

    async def hold_transaction() -> None:
        async with database.transaction() as transaction:
            transactions.append(transaction)
            await transaction.execute("UPDATE sample SET value = $1", ("cancelled",))
            entered.set()
            await asyncio.Future()

    task = asyncio.create_task(hold_transaction())
    try:
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert pool.connection.rolled_back == 1
        assert pool.connection.released == 1
        with pytest.raises(DatabaseError) as caught:
            await transactions[0].execute("UPDATE sample SET value = $1", ("late",))
        assert caught.value.code == "database.transaction-scope"
    finally:
        if not task.done():
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        await database.aclose()


async def test_close_is_idempotent_and_later_access_is_rejected(fake_pool) -> None:
    """Resource shutdown is bounded to one graceful pool close and terminal thereafter."""
    pool, _observed = fake_pool
    database = await PostgresDatabase.open(PostgresConfig(dsn="postgresql://u:p@db/app"))
    await database.aclose()
    await database.aclose()

    assert pool.closed == 1
    assert pool.terminated == 0
    with pytest.raises(DatabaseError) as caught:
        await database.fetch_one("SELECT 1")
    assert caught.value.code == "database.closed"


@pytest.mark.parametrize(
    "timeout",
    [0, -1, 300.1, float("inf"), float("nan"), True, 10**400, "10"],
)
async def test_direct_database_construction_validates_close_timeout(fake_pool, timeout) -> None:
    """The public pool wrapper cannot be constructed with an unsafe shutdown deadline."""
    pool, _observed = fake_pool
    with pytest.raises(ValueError, match="close_timeout"):
        PostgresDatabase(pool, close_timeout=timeout)


@pytest.mark.asyncio
@pytest.mark.parametrize("pool", [None, object()])
async def test_direct_database_construction_rejects_invalid_pool(pool) -> None:
    """Malformed pool objects are rejected before database state is created."""
    with pytest.raises(TypeError, match="acquire, close, and terminate"):
        PostgresDatabase(pool)  # type: ignore[arg-type]


async def test_open_rejects_invalid_config_before_attempting_driver_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A bad provider config is a caller error, not a redacted network failure."""
    called = False

    async def create_pool(**_kwargs: object) -> None:
        nonlocal called
        called = True

    monkeypatch.setattr(asyncpg, "create_pool", create_pool)
    with pytest.raises(TypeError, match="PostgresConfig"):
        await PostgresDatabase.open(object())  # type: ignore[arg-type]
    assert not called


async def test_pool_close_timeout_terminates_and_remains_idempotent(fake_pool, monkeypatch) -> None:
    """A stalled graceful close is bounded, followed by forced pool termination."""
    pool, _observed = fake_pool
    database = PostgresDatabase(pool, close_timeout=0.01)

    async def stalled_close() -> None:
        await asyncio.Future()

    monkeypatch.setattr(pool, "close", stalled_close)
    with pytest.raises(DatabaseError) as caught:
        await database.aclose()
    assert caught.value.code == "database.close-timeout"
    assert pool.terminated == 1
    await database.aclose()
    assert pool.terminated == 1


def test_postgres_config_rejects_invalid_pool_ranges_and_bounds() -> None:
    """Provider configuration is validated before secrets reach the driver."""
    with pytest.raises(ValidationError):
        PostgresConfig(dsn=SecretStr("postgresql://u:p@db/app"), min_pool_size=11, max_pool_size=1)
    with pytest.raises(ValidationError):
        PostgresConfig(dsn="postgresql://u:p@db/app", max_pool_size=129)
