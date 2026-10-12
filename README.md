# Baserunna

Baserunna is a web service that hosts small databases on object storage.

- Serializable transactions with strict serializability and snapshot isolation.
- Key Value API
- Large binary blobs stored out of band as chunked objects.
- Tables with schemas, primary keys, indexes, and schemas.
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

# Tables with a secondary index.
curl -s localhost:8080/v1/dbs/app/tables -H "$H" -d '{
  "name": "users", "primary_key": ["id"],
  "columns": [{"name":"id","type":"int","nullable":false},{"name":"email","type":"string"}],
  "indexes": [{"name":"by_email","columns":["email"],"unique":true}]}'
curl -s localhost:8080/v1/dbs/app/tables/users/rows -H "$H" -d '{"row":{"id":1,"email":"a@b.co"}}'
curl -s localhost:8080/v1/dbs/app/tables/users/rows/query -H "$H" -d '{"index":"by_email","eq":["a@b.co"]}'
```

## Configuration

All configuration is through environment variables (see SPEC §12):

| variable                                 | default                   |                                                                            |
| ---------------------------------------- | ------------------------- | -------------------------------------------------------------------------- |
| `BASERUNNA_STORE`                        | `fs:./data`               | `fs:<dir>` (single node), `mem:`, `s3://bucket/prefix`, `gs://…`, `az://…` |
| `BASERUNNA_LISTEN`                       | `0.0.0.0:8080`            |                                                                            |
| `BASERUNNA_ADVERTISE`                    | `http://127.0.0.1:<port>` | URL that peers use to reach this node                                      |
| `BASERUNNA_PUBLIC_URL`                   | advertise URL             | browser-facing URL (OAuth redirect, `Secure` cookies)                      |
| `BASERUNNA_CLUSTER_SECRET`               | none                      | protects internal endpoints. **Set it for multi-node.**                    |
| `BASERUNNA_SIGNUP`                       | `first_user_only`         | `open`, `first_user_only`, or `closed`                                     |
| `BASERUNNA_ADMIN_EMAIL` / `_PASSWORD`    | none                      | bootstrap an admin account                                                 |
| `BASERUNNA_GOOGLE_CLIENT_ID` / `_SECRET` | none                      | enables "Sign in with Google"                                              |
| `BASERUNNA_CACHE_MB`                     | 512                       | block cache budget                                                         |
| `BASERUNNA_LEASE_MS`                     | 15000                     | write-failover time bound                                                  |
| `BASERUNNA_LOG`                          | `warn`                    | `error` / `warn` / `info` / `debug`                                        |

For a cluster, run any number of nodes against the same bucket behind any
load balancer. S3 credentials come from the standard `AWS_*` variables;
S3-compatible stores need `AWS_ENDPOINT`.

## Examples

Each example starts real nodes on localhost and talks to them over HTTP:

| example                             | shows                                                                                         |
| ----------------------------------- | --------------------------------------------------------------------------------------------- |
| `cargo run --example todo_app`      | a multi-user app with tables, secondary indexes, filters, pagination, schema evolution        |
| `cargo run --example bank_ledger`   | concurrent interactive serializable transfers, conflict retries, idempotency, compare-and-set |
| `cargo run --example asset_store`   | large media files as streamed blobs, blob columns, HTTP range seeks                           |
| `cargo run --example saas_tenants`  | one database per tenant, scoped and read-only API keys, offboarding                           |
| `cargo run --example feature_flags` | a read-heavy config store, lease-served reads, `If-Match` compare-and-set                     |
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
cargo bench --bench throughput                                     # one node, thousands of active databases
scripts/mutants.sh                                                 # mutation testing (several hours)
```

## Layout

```
src/engine   storage engine: segments, manifest, versions, group commit, leases, compaction, GC, blobs
src/table    tuple encoding, schemas, rows, indexes, queries
src/ops.rs   the JSON transaction-op language
src/meta.rs  system database schema and metadata commands (dogfooded on the table layer)
src/node.rs  a service node: routing, auth, background work
src/api      axum HTTP API and the OpenAPI document;   src/console  admin console
src/sim      deterministic simulator, workloads and checkers
src/store    object stores: memory, filesystem, cloud (object_store)
```
