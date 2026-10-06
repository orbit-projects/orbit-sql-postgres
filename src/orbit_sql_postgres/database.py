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
"""asyncpg-backed ``SQLDatabase`` implementation with bounded reads and cleanup."""

from __future__ import annotations

import asyncio
import math
import re
from collections.abc import AsyncIterator, Iterable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import date, datetime, time
from decimal import Decimal
from typing import Protocol, cast
from uuid import UUID

import asyncpg  # type: ignore[import-untyped]
from orbit_sql import DatabaseError, SQLExecution, SQLParameters, SQLRow, SQLTransaction, SQLValue

from orbit_sql_postgres.config import PostgresConfig

_MAX_SQL_BYTES = 1_000_000
_MAX_ROWS = 10_000
_ROWCOUNT = re.compile(r"(?:^|\s)(\d+)$")


class _Record(Protocol):
    """Typed subset of asyncpg records consumed by the adapter."""

    def keys(self) -> Iterable[str]:
        """Return PostgreSQL result column names."""

    def values(self) -> Iterable[object]:
        """Return values in the same order as ``keys``."""


class _Cursor(Protocol):
    """Typed streaming surface of an asyncpg cursor factory."""

    def __aiter__(self) -> AsyncIterator[_Record]:
        """Yield cursor records incrementally from the active transaction."""


class _DriverTransaction(Protocol):
    """Transaction lifecycle methods provided by asyncpg."""

    async def start(self) -> None:
        """Begin the PostgreSQL transaction."""

    async def commit(self) -> None:
        """Commit the PostgreSQL transaction."""

    async def rollback(self) -> None:
        """Roll back the PostgreSQL transaction."""

    async def __aenter__(self) -> _DriverTransaction:
        """Begin when used as an async context manager."""

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> bool | None:
        """Commit or roll back context-managed transaction scope."""


class _Connection(Protocol):
    """Only the asyncpg connection methods used at this adapter boundary."""

    async def execute(self, query: str, *args: SQLValue) -> str:
        """Execute a parameterized statement and return command status."""

    async def fetchrow(self, query: str, *args: SQLValue) -> _Record | None:
        """Return one result record."""

    def cursor(self, query: str, *args: SQLValue, prefetch: int) -> _Cursor:
        """Create a transaction-bound cursor."""

    def transaction(self) -> _DriverTransaction:
        """Create a transaction context for this connection."""


class _Pool(Protocol):
    """Typed asyncpg pool lifecycle and connection acquisition surface."""

    def acquire(self) -> AbstractAsyncContextManager[_Connection]:
        """Acquire and release one connection through an async context manager."""

    async def close(self) -> None:
        """Gracefully close the pool."""

    def terminate(self) -> None:
        """Immediately terminate the pool and all its connections."""


class PostgresDatabase:
    """Pooled PostgreSQL access that implements Orbit Core's asynchronous SQL protocol."""

    def __init__(self, pool: _Pool, *, close_timeout: float = 10.0) -> None:
        """Wrap an already-open pool; applications should normally use ``open``."""
        if any(
            not callable(getattr(pool, method, None))
            for method in ("acquire", "close", "terminate")
        ):
            raise TypeError("pool must provide callable acquire, close, and terminate methods.")
        valid_timeout_type = not isinstance(close_timeout, bool) and isinstance(
            close_timeout, (int, float)
        )
        try:
            finite_timeout = valid_timeout_type and math.isfinite(close_timeout)
        except OverflowError:
            finite_timeout = False
        if not finite_timeout or not 0 < close_timeout <= 300:
            raise ValueError("close_timeout must be finite and between 0 and 300 seconds.")
        self._pool = pool
        self._close_timeout = float(close_timeout)
        self._loop = asyncio.get_running_loop()
        self._closed = False
        self._close_lock = asyncio.Lock()

    @classmethod
    async def open(cls, config: PostgresConfig) -> PostgresDatabase:
        """Create a bounded asyncpg pool without retaining or exposing the DSN."""
        if not isinstance(config, PostgresConfig):
            raise TypeError("config must be a PostgresConfig instance.")
        try:
            pool = cast(
                _Pool,
                await asyncpg.create_pool(
                    dsn=config.dsn.get_secret_value(),
                    min_size=config.min_pool_size,
                    max_size=config.max_pool_size,
                    command_timeout=config.command_timeout,
                    timeout=config.connect_timeout,
                    ssl=config.ssl,
                ),
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            raise DatabaseError(
                "database.connection-failed", "PostgreSQL connection could not be opened."
            ) from None
        return cls(pool, close_timeout=config.close_timeout)

    async def execute(self, statement: str, parameters: SQLParameters = ()) -> SQLExecution:
        """Execute a parameterized PostgreSQL statement and parse its row count."""
        self._validate_statement(statement)
        self._validate_parameters(parameters)
        self._ensure_open()
        try:
            async with self._pool.acquire() as connection:
                return await self._execute_on(connection, statement, parameters)
        except asyncio.CancelledError:
            raise
        except DatabaseError:
            raise
        except Exception:
            raise DatabaseError(
                "database.operation-failed", "PostgreSQL operation failed."
            ) from None

    async def fetch_one(self, statement: str, parameters: SQLParameters = ()) -> SQLRow | None:
        """Fetch a detached row using PostgreSQL's positional ``$n`` parameters."""
        self._validate_statement(statement)
        self._validate_parameters(parameters)
        self._ensure_open()
        try:
            async with self._pool.acquire() as connection:
                return await self._fetch_one_on(connection, statement, parameters)
        except asyncio.CancelledError:
            raise
        except DatabaseError:
            raise
        except Exception:
            raise DatabaseError(
                "database.operation-failed", "PostgreSQL operation failed."
            ) from None

    async def fetch_all(
        self, statement: str, parameters: SQLParameters = (), *, limit: int = 1_000
    ) -> tuple[SQLRow, ...]:
        """Stream at most ``limit + 1`` rows and reject oversized result sets."""
        self._validate_statement(statement)
        self._validate_parameters(parameters)
        self._validate_limit(limit)
        self._ensure_open()
        try:
            # PostgreSQL cursors require an active transaction; this local transaction bounds
            # network reads without buffering the entire result in the process.
            async with self._pool.acquire() as connection, connection.transaction():
                return await self._fetch_all_on(connection, statement, parameters, limit)
        except asyncio.CancelledError:
            raise
        except DatabaseError:
            raise
        except Exception:
            raise DatabaseError(
                "database.operation-failed", "PostgreSQL operation failed."
            ) from None

    async def _execute_on(
        self, connection: _Connection, statement: str, parameters: SQLParameters
    ) -> SQLExecution:
        """Execute on an acquired connection and redact any driver exception."""
        try:
            status = await connection.execute(statement, *parameters)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise _postgres_operation_error(exc) from None
        match = _ROWCOUNT.search(status)
        return SQLExecution(int(match.group(1)) if match is not None else -1)

    async def _fetch_one_on(
        self, connection: _Connection, statement: str, parameters: SQLParameters
    ) -> SQLRow | None:
        """Fetch and detach one result row from an acquired connection."""
        try:
            record = await connection.fetchrow(statement, *parameters)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise _postgres_operation_error(exc) from None
        return None if record is None else _to_sql_row(record)

    async def _fetch_all_on(
        self,
        connection: _Connection,
        statement: str,
        parameters: SQLParameters,
        limit: int,
    ) -> tuple[SQLRow, ...]:
        """Read a bounded cursor from an already transaction-bound connection."""
        try:
            cursor = connection.cursor(statement, *parameters, prefetch=min(limit + 1, 1_000))
            records: list[_Record] = []
            async for record in cursor:
                records.append(record)
                if len(records) > limit:
                    break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise _postgres_operation_error(exc) from None
        if len(records) > limit:
            raise DatabaseError(
                "database.result-limit", "SQL result exceeds the configured row limit."
            )
        return tuple(_to_sql_row(record) for record in records)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[SQLTransaction]:
        """Pin one pool connection until the transaction commits or rolls back."""
        self._ensure_open()
        acquire = self._pool.acquire()
        try:
            connection = await acquire.__aenter__()
        except asyncio.CancelledError:
            raise
        except Exception:
            raise DatabaseError(
                "database.acquire-failed", "PostgreSQL connection could not be acquired."
            ) from None
        transaction_context = connection.transaction()
        transaction = _PostgresTransaction(self, connection)
        try:
            try:
                await transaction_context.start()
            except asyncio.CancelledError:
                raise
            except Exception:
                raise DatabaseError(
                    "database.transaction-start-failed", "PostgreSQL transaction could not start."
                ) from None
            try:
                yield transaction
            except BaseException:
                try:
                    await transaction_context.rollback()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    raise DatabaseError(
                        "database.rollback-failed", "PostgreSQL transaction rollback failed."
                    ) from None
                raise
            else:
                try:
                    await transaction_context.commit()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if isinstance(exc, asyncpg.UniqueViolationError):
                        raise _postgres_operation_error(exc) from None
                    raise DatabaseError(
                        "database.commit-failed", "PostgreSQL transaction commit failed."
                    ) from None
        finally:
            transaction._active = False
            try:
                await acquire.__aexit__(None, None, None)
            except asyncio.CancelledError:
                raise
            except Exception:
                raise DatabaseError(
                    "database.release-failed", "PostgreSQL connection could not be released."
                ) from None

    async def aclose(self) -> None:
        """Drain and close the pool, terminating it when cleanup exceeds its bound."""
        async with self._close_lock:
            if self._closed:
                return
            self._closed = True
            try:
                await asyncio.wait_for(self._pool.close(), timeout=self._close_timeout)
            except asyncio.CancelledError:
                self._pool.terminate()
                raise
            except TimeoutError:
                self._pool.terminate()
                raise DatabaseError(
                    "database.close-timeout", "PostgreSQL pool did not drain before shutdown."
                ) from None
            except Exception:
                self._pool.terminate()
                raise DatabaseError(
                    "database.close-failed", "PostgreSQL pool could not close cleanly."
                ) from None

    def _ensure_open(self) -> None:
        if asyncio.get_running_loop() is not self._loop:
            raise DatabaseError(
                "database.loop-mismatch", "PostgreSQL resource belongs to another event loop."
            )
        if self._closed:
            raise DatabaseError("database.closed", "The database is closed.")

    @staticmethod
    def _validate_statement(statement: str) -> None:
        if not isinstance(statement, str) or not statement.strip():
            raise ValueError("SQL statement must be a non-empty string.")
        if len(statement.encode("utf-8")) > _MAX_SQL_BYTES:
            raise ValueError("SQL statement exceeds the 1,000,000-byte limit.")

    @staticmethod
    def _validate_parameters(parameters: SQLParameters) -> None:
        if not isinstance(parameters, tuple) or len(parameters) > 10_000:
            raise ValueError("SQL parameters must be a tuple with at most 10,000 values.")
        for value in parameters:
            if value is not None and not isinstance(
                value, (bool, int, float, str, bytes, Decimal, UUID, date, datetime, time)
            ):
                raise TypeError("SQL parameters must use supported portable scalar values.")
            if isinstance(value, float) and not math.isfinite(value):
                raise ValueError("SQL floating-point parameters must be finite.")
            if isinstance(value, Decimal) and not value.is_finite():
                raise ValueError("SQL decimal parameters must be finite.")

    @staticmethod
    def _validate_limit(limit: int) -> None:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= _MAX_ROWS:
            raise ValueError("SQL result limit must be between 1 and 10,000 rows.")


class _PostgresTransaction:
    """Task-owned facade over one acquired connection and an active asyncpg transaction."""

    def __init__(self, database: PostgresDatabase, connection: _Connection) -> None:
        self._database = database
        self._connection = connection
        self._owner = asyncio.current_task()
        self._active = True

    async def execute(self, statement: str, parameters: SQLParameters = ()) -> SQLExecution:
        """Execute a bound statement inside the connection-pinned transaction."""
        self._check()
        self._database._validate_statement(statement)
        self._database._validate_parameters(parameters)
        return await self._database._execute_on(self._connection, statement, parameters)

    async def fetch_one(self, statement: str, parameters: SQLParameters = ()) -> SQLRow | None:
        """Fetch one row inside the connection-pinned transaction."""
        self._check()
        self._database._validate_statement(statement)
        self._database._validate_parameters(parameters)
        return await self._database._fetch_one_on(self._connection, statement, parameters)

    async def fetch_all(
        self, statement: str, parameters: SQLParameters = (), *, limit: int = 1_000
    ) -> tuple[SQLRow, ...]:
        """Fetch bounded rows without acquiring a second connection."""
        self._check()
        self._database._validate_statement(statement)
        self._database._validate_parameters(parameters)
        self._database._validate_limit(limit)
        return await self._database._fetch_all_on(self._connection, statement, parameters, limit)

    def _check(self) -> None:
        if not self._active or asyncio.current_task() is not self._owner:
            raise DatabaseError(
                "database.transaction-scope", "Transaction is inactive or used by another task."
            )


def _to_sql_row(record: _Record) -> SQLRow:
    """Detach standard PostgreSQL scalar values into the Core row contract."""
    columns = tuple(record.keys())
    values = tuple(cast(SQLValue, value) for value in record.values())
    try:
        return SQLRow(columns, values)
    except (TypeError, ValueError):
        raise DatabaseError(
            "database.unsupported-result", "PostgreSQL returned an unsupported SQL value shape."
        ) from None


def _postgres_operation_error(error: Exception) -> DatabaseError:
    """Normalize uniqueness failures while redacting vendor messages and query details."""
    if isinstance(error, asyncpg.UniqueViolationError):
        return DatabaseError(
            "database.constraint-conflict", "Database uniqueness constraint was violated."
        )
    return DatabaseError("database.operation-failed", "PostgreSQL operation failed.")


__all__ = ["PostgresDatabase"]
