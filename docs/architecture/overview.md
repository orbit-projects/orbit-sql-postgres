# Orbit SQL PostgreSQL: architecture and boundaries

## Responsibility

`orbit-sql-postgres` is the optional asyncpg adapter at the provider-specific edge of Orbit's data
stack. It implements `orbit-sql`'s provider-neutral `SQLDatabase` contract for use by its
repositories. Core does not define the SQL contract, depend on this package, or install asyncpg.

```bash
pip install orbit-data orbit-sql orbit-sql-postgres
# Add the Core lifecycle plugin only when the application uses it:
pip install 'orbit-sql-postgres[core]'
```

Configure its typed `PostgresConfig` from a managed secret source. For explicit application-managed
connections, register `PostgresAdapter` with `SQLAdapterRegistry` and close the returned database
during shutdown. For Core-managed wiring, register `PostgresPlugin(config)` with the application;
it lazily provides the SQL capability under `orbit_sql.SQL_DATABASE_KEY` and lets Core close the
pool during container cleanup. Setup performs no network I/O. TLS verification is enabled by
default. `PostgresDatabase.open` rejects non-`PostgresConfig` inputs before attempting I/O; direct
construction validates the supplied pool's required lifecycle methods. Failover, migrations,
server-version compatibility, and deployment TLS policy still require provider-specific integration
tests. The examples read `ORBIT_POSTGRES_DSN` from the process environment; configure that value
through the deployment's secret-management mechanism and never commit a credential-bearing DSN.

Transactions pin one pooled connection. Cancellation inside a transaction rolls back and releases
that connection before the cancellation propagates. Pool shutdown has a validated finite deadline;
if graceful close exceeds it or shutdown is cancelled, the pool is terminated. If cancellation or a
connection failure occurs while a commit is in flight, the final commit outcome may be unknown—do
not blindly retry a write unless the operation is idempotent or protected by an application-level
deduplication key.

Primary routing is the default for every query, preserving read-your-writes consistency. A caller
may opt into `SQLReadPreference.REPLICA` for eventual-consistency reads when weighted replicas are
configured. The adapter enforces PostgreSQL read-only transaction mode for those operations; it
does not infer safety from SQL text. Selection is weighted round-robin, with no implicit fallback
or replay to primary on a replica error. Replica lag and health remain visible operational concerns.

```python
from datetime import UTC, datetime
from decimal import Decimal
from os import environ
from uuid import UUID

from pydantic import BaseModel
from orbit_sql import SQLAdapterRegistry, SQLRepository, SQLUnitOfWork
from orbit_sql_postgres import PostgresAdapter, PostgresConfig


class Note(BaseModel):
    id: UUID
    amount: Decimal
    created_at: datetime


adapters = SQLAdapterRegistry()
adapters.register(PostgresAdapter())
database = await adapters.connect("postgres", PostgresConfig(dsn=environ["ORBIT_POSTGRES_DSN"]))
try:
    repository = SQLRepository(database, Note, table="notes", primary_key="id")
    async with SQLUnitOfWork(database, (repository,)) as unit:
        await unit.repository(Note).add(
            Note(
                id=UUID("12345678-1234-5678-1234-567812345678"),
                amount=Decimal("12.50"),
                created_at=datetime.now(UTC),
            )
        )
finally:
    await database.aclose()
```

The Core plugin path is an alternative to the manual registry path above:

```python
from os import environ

from orbit import Application, ApplicationConfig
from orbit_sql import SQL_DATABASE_KEY
from orbit_sql_postgres import PostgresConfig
from orbit_sql_postgres.plugin import PostgresPlugin

application = Application(ApplicationConfig(name="orders"))
application.register_plugin(PostgresPlugin(PostgresConfig(dsn=environ["ORBIT_POSTGRES_DSN"])))
await application.startup()
database = await application.container.aresolve(SQL_DATABASE_KEY)
try:
    await database.execute("SELECT 1")
finally:
    await application.stop()  # Core closes the pool resource.
```

The example assumes the `notes` table has already been provisioned; schema migrations are a
separate capability. Application code depends on `orbit-data`/`orbit-sql` contracts, while the
PostgreSQL package supplies only the selected SQL provider.

This package is in alpha and its API is not stable. It supports Python 3.11 through 3.14. The
default suite uses a fake asyncpg pool; an opt-in live check exercises query execution, bounded
cursor iteration, transaction rollback, and cleanup against a disposable PostgreSQL database. It
creates and removes only a uniquely named table. Remote databases are skipped unless explicitly
enabled, and TLS remains on when the target is not loopback.

## Declared dependencies

The following dependency declarations come from the checked-in manifests. Optional groups and development dependencies are called out separately.

### `pyproject.toml`
- `orbit-sql>=0.1.0a1,<0.2`
- `asyncpg>=0.30,<1`
- Optional `core` extra: `orbit-core>=0.1.0a1,<0.2` for `PostgresPlugin`.
- Development dependencies: Core for plugin tests, plus pytest, pytest-asyncio, Ruff, and mypy.

Declared dependencies do not mean that optional providers or services are bundled with this package.

## Implementation layout

Representative implementation files in this checkout:

- `src/orbit_sql_postgres/__init__.py`
- `src/orbit_sql_postgres/adapter.py`
- `src/orbit_sql_postgres/config.py`
- `src/orbit_sql_postgres/database.py`
- `src/orbit_sql_postgres/plugin.py`

## Public contract and scope

The README does not contain a separately headed architecture section. Its responsibility statement above and the public source files define the implemented scope; this guide adds no behavior beyond that description.

## Boundary rules

Keep provider SDKs, credentials, transports, and provider-specific error translation in provider adapters. Keep reusable capability contracts in the matching capability package and lifecycle orchestration in Core. Apply the relevant layer for this repository and preserve the dependency direction shown above.
