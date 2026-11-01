# Deploying Baserunna

A production cluster consists of stateless nodes behind an HTTP load balancer
that share a single object-storage bucket. Nodes are configured through
environment variables (see [`README.md`](README.md#configuration) and
[`SPEC.md`](SPEC.md) §12).

```
clients ──TLS──▶ load balancer ──HTTP──▶ node, node, node ──▶ bucket
                                          ▲      ▲      ▲
                                          └──────┴──────┘
                                   forwarding (private network)
```

## Container image

The [`Dockerfile`](Dockerfile) builds the release binary and runs it as an
unprivileged user in a minimal Debian image:

```sh
docker build -t baserunna .
docker run -p 8080:8080 \
  -e BASERUNNA_STORE=s3://my-bucket/baserunna \
  -e BASERUNNA_CLUSTER_SECRET=$(openssl rand -hex 32) \
  -e BASERUNNA_ADVERTISE=http://10.0.1.17:8080 \
  -e BASERUNNA_PUBLIC_URL=https://db.example.com \
  -e AWS_REGION=us-east-1 \
  baserunna
```

Each node in a cluster requires:

- identical `BASERUNNA_STORE` and `BASERUNNA_CLUSTER_SECRET` values;
- a node-specific `BASERUNNA_ADVERTISE` URL reachable by peer nodes on a
  private network (a pod or instance IP, not the load balancer);
- `BASERUNNA_PUBLIC_URL`: the browser-facing URL used for OAuth redirects,
  email links, and `Secure` session cookies (if `https://`).

`fs:` stores are restricted to a single development process (SPEC §12).

## Fly.io (single node)

[`fly.toml`](fly.toml) runs a single node on a Fly machine against a
[Tigris](https://fly.io/docs/tigris/) bucket. Fly's proxy terminates TLS, and
the machine stops if idle and starts on the next request. Initial setup with
[`flyctl`](https://fly.io/docs/flyctl/install/):

```sh
fly apps create baserunna
fly storage create --app baserunna --name <bucket>   # sets the AWS_* secrets
fly secrets set --app baserunna --stage \
  BASERUNNA_STORE=s3://<bucket> \
  BASERUNNA_ADMIN_EMAIL=you@example.com \
  BASERUNNA_ADMIN_PASSWORD=$(openssl rand -base64 24)
fly deploy --ha=false
```

`fly storage create` creates a Global Tigris bucket, which provides strong
consistency only within a single region. A cluster with nodes in multiple
regions requires a Single-region or Multi-region bucket created in the Tigris
dashboard or with `tigris mk --locations` so that conditional manifest writes
are serialized across all nodes.

Set the admin secrets before the first deploy: under the default
`first_user_only` sign-up policy, the first user to sign up otherwise becomes
admin. Keep the app at 1 machine (`--ha=false`); a multi-node cluster requires
a private advertise address and a cluster secret. Background work (garbage
collection, TTL expiry, stream retention) runs only while the machine is
running.

Use a `performance` machine for sustained load. A `shared-cpu-1x` machine
serves ~3,900 cached GETs/s until its CPU credits are exhausted after a few
minutes, then drops to ~300 GETs/s.

[`.github/workflows/deploy.yml`](.github/workflows/deploy.yml) deploys
every push to `main`, building on Fly's remote builder. It needs a
`FLY_API_TOKEN` repository secret (`fly tokens create deploy --app
baserunna`).

## TLS

Terminate TLS at the load balancer or ingress. Nodes communicate over plain
HTTP with the load balancer and with each other.

Forwarded requests between nodes include client credentials, so advertised
node addresses must be on a private network (such as a VPC, Kubernetes pod
network, or mTLS service mesh). Internal endpoints (`/internal/v1/…`) require
the cluster secret, and node-to-node traffic is not encrypted by Baserunna.

## Load balancer

Any HTTP load balancer works; requests may land on any node, and nodes
forward database requests to the node holding the database's lease. Set up:

- **Health check**: `GET /healthz`. It returns 200 while the node serves
  and 503 during shutdown.
- **Disable sticky sessions and automatic retries of non-idempotent
  requests** (POST, PUT, DELETE). Retrying a write without an idempotency key
  can apply it twice (SPEC §6.2).
- **Timeouts and body sizes**: Set the load balancer idle timeout above
  50 s so that `wait_ms` long polls on change feeds and streams do not
  time out before the server responds. On `/v1/dbs/*/kv/*` and
  `/v1/dbs/*/blobs`, allow request bodies and streaming transfers up to
  `blob_max` (5 GiB).
- **Client addresses**: sign-in throttling is per client address. Behind
  a proxy that appends the client to `X-Forwarded-For`, set
  `BASERUNNA_TRUSTED_PROXY_HOPS` to the number of such proxies (usually 1).
  Leave it at 0 if nodes face clients directly so that clients cannot spoof
  their IP address.
- Optionally block `/internal/` at the load balancer; nodes reach those
  endpoints directly.

## Logs

Nodes write JSON log lines to stderr (see SPEC §12). At the default `info`
level, each node logs every request it handles; set `BASERUNNA_LOG=warn` to
log only warnings and errors, or a filter such as `info,baserunna::engine=debug`
to debug one module. Filter by the `request` span's `request_id` to trace a
request across nodes. If a reverse proxy sets `x-request-id` to a 1–64
character string matching `[A-Za-z0-9._:-]`, nodes preserve that ID;
otherwise, nodes generate a fresh ID.

## Mail

Email verification and password resets require an outbound mail webhook. Set
`BASERUNNA_MAIL_WEBHOOK_URL` to an HTTP endpoint that accepts
`POST {"from", "to", "subject", "text"}` (with optional
`BASERUNNA_MAIL_WEBHOOK_TOKEN` bearer authentication and `BASERUNNA_MAIL_FROM`
sender address). Email links use `BASERUNNA_PUBLIC_URL`. If
`BASERUNNA_MAIL_WEBHOOK_URL` is unset, the verification and password-reset
endpoints return `404`, and Google sign-in cannot link to unverified password
accounts. Outbound emails are sent asynchronously with retries and are not
persisted across node restarts.

## Shutdown

On SIGTERM (or SIGINT) a node:

1. fails its health check for `BASERUNNA_DRAIN_DELAY_MS` (default 0) while
   still serving, so the load balancer stops sending it requests;
2. stops accepting connections, ends active `wait_ms` long polls immediately,
   and waits up to `BASERUNNA_SHUTDOWN_TIMEOUT_MS` (default 20 s) for
   in-flight requests to complete;
3. leaves the membership table and releases its database leases so that other
   nodes can take over writes immediately.

Set the drain delay to at least the load balancer's health check interval
times its unhealthy threshold, and give the orchestrator a grace period
longer than both settings together (on Kubernetes,
`terminationGracePeriodSeconds`). Uncommitted interactive transactions open on
a stopping node are aborted (`404 txn_not_found`). If a node crashes,
databases leased by that node become writable again after their leases expire
(SPEC §5.7).

## Upgrades

If a new release changes the storage format, older nodes cannot read manifests
written by newer nodes. To upgrade a cluster across a format change, stop all
nodes before starting the new version.

## Bucket

### Requirements

Baserunna supports [Tigris](https://www.tigrisdata.com/), which passes the
object-store conformance suite and failover check (SPEC §13.5). Other
S3-compatible stores are untested. Dedicate a separate bucket or key prefix
(`s3://bucket/prefix`) to each cluster.

Credentials, region, and endpoint are read from standard `AWS_*` environment
variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, and
`AWS_ENDPOINT_URL_S3`).

At startup, each node runs conditional-write preflight checks against the
store and refuses to start if they fail. To run the full conformance suite and
failover check against a bucket:

```sh
BASERUNNA_TEST_STORE=s3://<bucket> cargo test --release --test store_conformance
BASERUNNA_STORE=s3://<bucket> cargo run --release --example cluster_check -- 180
```

Both commands write to a temporary prefix and delete all test objects before
exiting.

### Permissions

Nodes require permission to read, write, delete, and list objects under the
cluster prefix.

Example IAM policy for prefix `baserunna/` in `my-bucket`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::my-bucket/baserunna/*"
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::my-bucket",
      "Condition": { "StringLike": { "s3:prefix": ["baserunna/*"] } }
    }
  ]
}
```

### Lifecycle rules and versioning

Baserunna deletes obsolete segments, unreferenced blobs, and purged databases
automatically after grace periods that protect in-flight reads (SPEC §5.8).

- **Do not configure object expiration rules** on the cluster prefix. Expiring
  an object referenced by a manifest causes data loss.
- **Abort incomplete multipart uploads** after 1 day. Baserunna writes each
  object in a single `PUT` request (blobs are stored as 8 MiB chunks), so this
  rule only cleans up incomplete uploads from external tools.
- **Disable bucket versioning.** Every commit overwrites the database
  manifest, so enabling versioning retains a noncurrent version per commit. If
  versioning is required, expire noncurrent versions after 1 day.
- **Use a storage class with immediate read access** (do not use archive
  tiers).

S3 lifecycle configuration with the rule above:

```json
{
  "Rules": [
    {
      "ID": "abort-incomplete-multipart-uploads",
      "Status": "Enabled",
      "Filter": { "Prefix": "baserunna/" },
      "AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 1 }
    }
  ]
}
```

## Limits

Rate limits and memory budgets are enforced per node, so an N-node cluster
allows up to N times each configured rate. Set a rate limit to 0 to disable it.
`BASERUNNA_DB_MEMORY_MB` has a minimum value of 16 and cannot be disabled. If
memory usage exceeds this budget after idle databases are unloaded, the node
enforces max-min fair sharing across users: it rejects interactive transactions
from users at or above their fair share with `503` and aborts idle transactions
from users above their fair share (SPEC §5.12).

- `BASERUNNA_DB_MAX_MB` (default `10240`): maximum size of each database in MiB (segments and blobs).
- `BASERUNNA_DB_MEMORY_MB` (default `256`): per-node memory budget in MiB for open databases and transactions.
- `BASERUNNA_USER_RPS` (default `500`): requests per second per user.
- `BASERUNNA_API_KEY_RPS` (default `500`): requests per second per API key.
- `BASERUNNA_AUTH_RPS_PER_CLIENT` (default `1`): authentication requests per second per client IP.
- `BASERUNNA_SIGNUPS_PER_HOUR_PER_CLIENT` (default `20`): account sign-ups per hour per client IP.
- `BASERUNNA_HASH_CONCURRENCY` (default: CPU count): maximum concurrent password hashes.

## Configuration

Nodes are configured via environment variables (see [`SPEC.md`](SPEC.md) §12 for the complete list):

- `BASERUNNA_STORE` (default `fs:./data`): `fs:<dir>`, `mem:`, or `s3://bucket/prefix` (Tigris).
- `BASERUNNA_LISTEN` (default `0.0.0.0:8080`): HTTP listen address.
- `BASERUNNA_ADVERTISE` (default `http://127.0.0.1:<port>`): URL that peer nodes use to reach this node.
- `BASERUNNA_PUBLIC_URL` (default: advertise URL): browser-facing URL for OAuth redirects, email links, and `Secure` cookies.
- `BASERUNNA_CLUSTER_SECRET` (default: unset): shared secret protecting internal endpoints in multi-node clusters.
- `BASERUNNA_SIGNUP` (default `first_user_only`): `open`, `first_user_only`, or `closed`.
- `BASERUNNA_ADMIN_EMAIL` / `BASERUNNA_ADMIN_PASSWORD` (default: unset): bootstrap credentials for an admin account.
- `BASERUNNA_GOOGLE_CLIENT_ID` / `BASERUNNA_GOOGLE_CLIENT_SECRET` (default: unset): Google OAuth credentials.
- `BASERUNNA_MAIL_WEBHOOK_URL` (default: unset): HTTP webhook URL for verification and password-reset emails.
- `BASERUNNA_LOG` (default `info`): log level (`error`, `warn`, `info`, `debug`, `trace`, or `off`) or a `tracing` filter such as `info,baserunna::engine=debug`.
- `BASERUNNA_LOG_FORMAT` (default `json`): `json` or `text`.
