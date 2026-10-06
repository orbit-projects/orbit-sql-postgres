# Orbit SQL PostgreSQL: operations and security

This guide organizes runtime behavior documented by the package. It does not certify production readiness. Verify provider/client versions, permissions, transport security, limits, and failure behavior in the target environment before release.

## Configuration surface

Environment names found in the package README:

- `ORBIT_POSTGRES_DSN`

Use the package README's constructor and deployment examples. Store credentials in a secret manager and avoid logging credentials, raw provider errors, request data, or opaque cursors.

## Lifecycle, failure behavior, and limits

`orbit-sql-postgres` is the optional asyncpg adapter at the provider-specific edge of Orbit's data
stack. It implements `orbit-sql`'s provider-neutral `SQLDatabase` contract for use by its
repositories. Core does not define the SQL contract, depend on this package, or install asyncpg.

```bash
pip install orbit-core orbit-data orbit-sql orbit-sql-postgres
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
if graceful close exceeds it, the pool is terminated. If cancellation or a connection failure
occurs while a commit is in flight, the final commit outcome may be unknown—do not blindly retry a
write unless the operation is idempotent or protected by an application-level deduplication key.

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
from orbit_sql_postgres import PostgresConfig, PostgresPlugin

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

This package is pre-alpha and its API is not stable. It supports Python 3.11 through 3.14. The
default suite uses a fake asyncpg pool; an opt-in live check exercises query execution, bounded
cursor iteration, transaction rollback, and cleanup against a disposable PostgreSQL database. It
creates and removes only a uniquely named table. Remote databases are skipped unless explicitly
enabled, and TLS remains on when the target is not loopback.

## Production validation

Validate startup/shutdown cleanup, timeout and cancellation behavior, concurrency and payload bounds where applicable, secret rotation and least-privilege access, data durability, backup/restore, and failover against the selected provider. Do not infer distributed or durable guarantees from an in-process API or fake-client tests.
