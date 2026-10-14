# Baserunna

Baserunna is a web service that hosts small databases on object storage.

- Serializable transactions with strict serializability and snapshot isolation.
- Key Value API
- Large binary blobs stored out of band as chunked objects.
- Tables with schemas, primary keys and secondary indexes, with updates and
  deletes of many rows by query in one transaction.
- Values and rows with times to live, expired lazily on read and by compaction.
- Change feeds of keys and rows, with long polls.
- Organizations: databases shared by members with roles (reader, writer,
  admin, owner).
- Account self-service: password changes, sessions, account deletion,
  email verification and password resets.
- JSON logs with request ids that follow requests across nodes.
- Stateless nodes: kill any node at any time.
- Verification with deterministic simulation testing

Design details are in [`SPEC.md`](SPEC.md).
The API reference is [`openapi.json`](openapi.json), served at `GET /v1/openapi.json`.

## Quick start

```sh
cargo run --release                         # listens on :8080, stores data in ./data
open http://127.0.0.1:8080/console           # first account to sign up becomes admin
```

```sh
# Sign up and keep the session token.
TOKEN=$(curl -s localhost:8080/v1/auth/signup \
  -d '{"email":"me@example.com","password":"correct-horse"}' | jq -r .token)
H="Authorization: Bearer $TOKEN"

curl -s localhost:8080/v1/dbs -H "$H" -d '{"name":"app"}'

# Raw values (large bodies become blobs automatically).
curl -s -X PUT localhost:8080/v1/dbs/app/kv/greeting -H "$H" --data-binary 'hello'
curl -s localhost:8080/v1/dbs/app/kv/greeting -H "$H"

# An atomic multi-op transaction.
curl -s localhost:8080/v1/dbs/app/txn -H "$H" -d '{
  "ops": [
    {"op": "check", "key": "greeting", "exists": true},
    {"op": "increment", "key": "visits", "by": 1},
    {"op": "scan", "prefix": "", "limit": 10}
  ],
  "idempotency_key": "request-123"
}'

# A value that expires in an hour, and the changes since version 0 (waiting up to 5 s for one).
curl -s -X PUT "localhost:8080/v1/dbs/app/kv/session?ttl_ms=3600000" -H "$H" --data-binary 'token'
curl -s "localhost:8080/v1/dbs/app/changes?after=0&wait_ms=5000" -H "$H"

# An organization whose members share databases, addressed as org.name.
curl -s localhost:8080/v1/orgs -H "$H" -d '{"name":"acme"}'
curl -s localhost:8080/v1/orgs/acme/members -H "$H" -d '{"email":"ann@example.com","role":"writer"}'
curl -s localhost:8080/v1/dbs -H "$H" -d '{"name":"shared","org":"acme"}'
curl -s -X PUT localhost:8080/v1/dbs/acme.shared/kv/hello -H "$H" --data-binary 'team'

# Tables with a secondary index.
curl -s localhost:8080/v1/dbs/app/tables -H "$H" -d '{
  "name": "users", "primary_key": ["id"],
  "columns": [{"name":"id","type":"int","nullable":false},{"name":"email","type":"string"}],
  "indexes": [{"name":"by_email","columns":["email"],"unique":true}]}'
curl -s localhost:8080/v1/dbs/app/tables/users/rows -H "$H" -d '{"row":{"id":1,"email":"a@b.co"}}'
curl -s localhost:8080/v1/dbs/app/tables/users/rows/query -H "$H" -d '{"index":"by_email","eq":["a@b.co"]}'
curl -s localhost:8080/v1/dbs/app/tables/users/rows/delete -H "$H" -d '{"where":{"filter":{"col":"email","op":"prefix","value":"a@"}}}'
```

## Configuration

All configuration is through environment variables. The main ones are
below; [`SPEC.md`](SPEC.md) §12 lists every one (caches, leases, quotas,
rate limits, shutdown, logging, mail):

| variable                                 | default                   |                                                                            |
| ---------------------------------------- | ------------------------- | -------------------------------------------------------------------------- |
| `BASERUNNA_STORE`                        | `fs:./data`               | `fs:<dir>` (single node), `mem:`, `s3://bucket/prefix`, `gs://…`, `az://…` |
| `BASERUNNA_LISTEN`                       | `0.0.0.0:8080`            |                                                                            |
| `BASERUNNA_ADVERTISE`                    | `http://127.0.0.1:<port>` | URL that peers use to reach this node                                      |
| `BASERUNNA_PUBLIC_URL`                   | advertise URL             | browser-facing URL (OAuth redirect, mailed links, `Secure` cookies)        |
| `BASERUNNA_CLUSTER_SECRET`               | none                      | protects internal endpoints. **Set it for multi-node.**                    |
| `BASERUNNA_SIGNUP`                       | `first_user_only`         | `open`, `first_user_only`, or `closed`                                     |
| `BASERUNNA_ADMIN_EMAIL` / `_PASSWORD`    | none                      | bootstrap an admin account                                                 |
| `BASERUNNA_GOOGLE_CLIENT_ID` / `_SECRET` | none                      | enables "Sign in with Google"                                              |
| `BASERUNNA_MAIL_WEBHOOK_URL`             | none                      | enables verification and password reset mail                               |
| `BASERUNNA_LOG`                          | `info`                    | `error` / `warn` / `info` (logs every request) / `debug` / `off`           |

For a cluster, run any number of nodes against the same bucket behind any
load balancer. [`DEPLOYING.md`](DEPLOYING.md) covers TLS, load balancers, the
container image, bucket permissions and lifecycle rules.

## Examples

Each example starts real nodes on localhost and talks to them over HTTP:

| example                             | shows                                                                                         |
| ----------------------------------- | --------------------------------------------------------------------------------------------- |
| `cargo run --example todo_app`      | a multi-user app with tables, secondary indexes, filters, pagination, schema evolution, batch updates and deletes |
| `cargo run --example bank_ledger`   | concurrent interactive serializable transfers, conflict retries, idempotency, compare-and-set |
| `cargo run --example asset_store`   | large media files as streamed blobs, blob columns, HTTP range seeks                           |
| `cargo run --example saas_tenants`  | one database per tenant, scoped and read-only API keys, offboarding                           |
| `cargo run --example feature_flags` | a read-heavy config store, lease-served reads, `If-Match` compare-and-set, expiring overrides, a change feed watcher |
| `cargo run --example multi_node`    | three nodes, forwarding, killing a node mid-flight, failover without data loss                |

## Testing

```sh
cargo test                                   # unit, property, fuzz, lint, OpenAPI, cost and simulation tests
SIM_SEEDS=500 cargo test --release --test sim                      # longer simulation campaigns
cargo run --release --example sim_tool -- campaign all 3000        # very long campaigns
cargo run --release --example sim_tool -- trace bank 101863        # print a run's full trace
SIM_SEED=4242 cargo test --test sim sim_bank -- --nocapture        # reproduce one seed
cd fuzz && cargo +nightly fuzz run http_txn -- -rss_limit_mb=1500  # coverage-guided fuzzing (optional)
cargo run --release --example cost_model                           # store requests per operation, $/DB/month
cargo run --release --example amplification                        # compaction's write/space/read amplification
cargo bench --bench throughput                                     # one node, thousands of active databases
scripts/mutants.sh                                                 # mutation testing (several hours)
```

## Layout

```
src/engine   storage engine: compressed segments, manifest, versions, group commit, commit records,
             leases, expiry, change feeds, range tombstones, leveled compaction, local disk tier,
             store clock, GC, blobs, quotas
src/table    tuple encoding, schemas, rows, indexes, queries
src/ops.rs   the JSON transaction-op language
src/meta     system database schema, metadata commands, organizations and roles (dogfooded on the table layer)
src/node.rs  a service node: routing, auth, accounts, background work;   src/limits.rs  rate limits
src/log.rs   JSON logs with request ids;   src/mail.rs  outbound mail
src/api      axum HTTP API and the OpenAPI document;   src/console  admin console
src/sim      deterministic simulator, workloads and checkers
src/store    object stores: memory, filesystem, cloud (object_store)
```
