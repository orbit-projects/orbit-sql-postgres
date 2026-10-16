# Orbit SQL PostgreSQL

`orbit-sql-postgres` is the optional asyncpg adapter at the provider-specific edge of Orbit's data
stack. It implements `orbit-sql`'s provider-neutral `SQLDatabase` contract for use by its
repositories. Core does not define the SQL contract, depend on this package, or install asyncpg.
The adapter and `SQLAdapterRegistry` work without Core. `PostgresPlugin` is an optional Core
integration imported from `orbit_sql_postgres.plugin` and installed with the `[core]` extra.

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

All reads use the primary pool by default for read-your-writes behavior. Configure optional weighted
`PostgresReadReplica` entries (up to 32, with a combined configured maximum of 1,024 pool
connections across all endpoints) and explicitly select one for eventual-consistency reads with
`read_preference=SQLReadPreference.REPLICA`. Orbit wraps replica reads and replica transactions in
PostgreSQL read-only transactions, so the database rejects writes even when arbitrary SQL is passed
to a fetch method. Replica selection uses weighted round-robin; replica errors are surfaced without
replaying the query on primary, and replica lag or health detection is not hidden by automatic
fallback. Applications should use primary reads after writes when they need read-your-writes
consistency.

```python
from orbit_sql import SQLReadPreference

rows = await database.fetch_all(
    "SELECT id, status FROM jobs WHERE status = $1",
    ("complete",),
    read_preference=SQLReadPreference.REPLICA,
)
```

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

With Orbit CLI installed in the local workspace, the registered plugin exposes **Check PostgreSQL
database**. This read-only action runs SELECT 1 using a short-lived pool and closes that pool
before returning. It never prints the DSN or driver error text.

The example assumes the `notes` table has already been provisioned; schema migrations are a
separate capability. Application code depends on `orbit-data`/`orbit-sql` contracts, while the
PostgreSQL package supplies only the selected SQL provider.

This package is in alpha and its API is not stable. It supports Python 3.11 through 3.14. The
default suite uses a fake asyncpg pool; an opt-in live check exercises query execution, bounded
cursor iteration, transaction rollback, and cleanup against a disposable PostgreSQL database. It
creates and removes only a uniquely named table. Remote databases are skipped unless explicitly
enabled, and TLS remains on when the target is not loopback.

## Documentation

The package-specific guides cover [architecture](docs/architecture/overview.md), [operations and security](docs/operations/README.md), and [development](docs/development/README.md), with [security guidance](docs/security/overview.md). The [documentation index](docs/README.md) links to the full package overview and project policies.

## Development

```bash
python -m pip install -e '.[dev]'
pytest
ruff check .
mypy
```

Run the live check against a disposable local database with the adapter's test credentials:

```bash
ORBIT_POSTGRES_DSN='postgresql://orbit_test:orbit_test@127.0.0.1:55432/orbit_test' \
  pytest -q tests/test_live_postgres.py
```

Licensed under Apache-2.0.

