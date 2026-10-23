# Deploying Baserunna

A production cluster is a few identical, stateless nodes behind a load
balancer, all pointed at one bucket. This guide covers what each piece
needs. Configuration is through environment variables; the full list is in
the [README](README.md#configuration) and SPEC §12.

```
clients ──TLS──▶ load balancer ──HTTP──▶ node, node, node ──▶ bucket
                                          ▲      ▲      ▲
                                          └──────┴──────┘
                                   forwarding (private network)
```

## Container image

The [`Dockerfile`](Dockerfile) builds the release binary and copies it into
a slim Debian image that runs it as an unprivileged user:

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

Every node of a cluster needs:

- the same `BASERUNNA_STORE` and `BASERUNNA_CLUSTER_SECRET`;
- its own `BASERUNNA_ADVERTISE`: the URL other nodes use to reach it
  directly (a pod or instance IP, not the load balancer);
- `BASERUNNA_PUBLIC_URL`: the URL browsers use. It sets the OAuth redirect
  and makes session cookies `Secure` when it is `https://`.

`fs:` stores are for a single development process only (SPEC §12).

## Fly.io (single node)

[`fly.toml`](fly.toml) runs one node on a Fly machine against a
[Tigris](https://fly.io/docs/tigris/) bucket, which supports the
conditional writes Baserunna needs. Fly's proxy terminates TLS, and the
machine stops when idle and starts on the next request. One-time setup,
with [`flyctl`](https://fly.io/docs/flyctl/install/):

```sh
fly apps create baserunna
fly storage create --app baserunna --name <bucket>   # sets the AWS_* secrets
fly secrets set --app baserunna --stage \
  BASERUNNA_STORE=s3://<bucket> \
  BASERUNNA_ADMIN_EMAIL=you@example.com \
  BASERUNNA_ADMIN_PASSWORD=$(openssl rand -base64 24)
fly deploy --ha=false
```

Set the admin secrets before the first deploy: under the default
`first_user_only` sign-up policy, whoever signs up first otherwise becomes
admin. Keep the app at one machine (`--ha=false`); a second one would need
a private advertise address and a cluster secret. Background work (garbage
collection, TTL expiry, stream retention) runs only while the machine is
up.

Use a `performance` machine for sustained load. A `shared-cpu-1x` machine
serves ~3,900 cached GETs/s until its CPU credits are exhausted after a few
minutes, then drops to ~300 GETs/s.

[`.github/workflows/deploy.yml`](.github/workflows/deploy.yml) deploys
every push to `main`, building on Fly's remote builder. It needs a
`FLY_API_TOKEN` repository secret (`fly tokens create deploy --app
baserunna`).

## TLS

Terminate TLS at the load balancer or ingress. Nodes speak plain HTTP, to
the load balancer and to each other.

Forwarded requests between nodes carry the client's credentials, so the
nodes' advertised addresses must be on a private network (a VPC, a
Kubernetes pod network, or a service mesh that adds mTLS). Internal
endpoints (`/internal/v1/…`) additionally require the cluster secret, but
nothing else on the node-to-node path is encrypted by Baserunna itself.

## Load balancer

Any HTTP load balancer works; requests may land on any node, and nodes
forward database requests to the node holding the database's lease. Set up:

- **Health check**: `GET /healthz`. It returns 200 while the node serves
  and 503 once it starts shutting down.
- **No sticky sessions** and **no automatic retries of non-idempotent
  requests** (POST, PUT, DELETE). A retried write could apply twice;
  clients retry safely with idempotency keys (SPEC §6.2).
- **Timeouts and body sizes**: blob uploads and downloads stream up to
  `blob_max` (5 GiB), so allow long idle timeouts and large request bodies
  on `/v1/dbs/*/kv/*` and `/v1/dbs/*/blobs`. Change feed and stream reads
  with `wait_ms` send nothing for up to 50 s, so the idle timeout must
  exceed 50 s; the common 60 s default works.
- **Client addresses**: sign-in throttling is per client address. Behind
  a proxy that appends the client to `X-Forwarded-For`, set
  `BASERUNNA_TRUSTED_PROXY_HOPS` to the number of such proxies (usually 1).
  Leave it at 0 when nodes face clients directly, or clients could forge
  their address.
- Optionally block `/internal/` at the load balancer; nodes reach those
  endpoints directly.

## Logs

Nodes log JSON lines to stderr (format in SPEC §12), and at the default
level, `info`, every request on each node it reaches; `BASERUNNA_LOG=warn`
keeps only problems. To follow a request through the cluster, filter on
its `request_id`. To tie the nodes' logs to your proxy's, have the proxy
send its request id as `x-request-id`: nodes keep ids of 1-64 characters
of `[A-Za-z0-9._:-]` and replace any other with their own, so ids with
other characters (an AWS ALB trace id's `=` and `;`, for example) need
rewriting first.

## Mail

Email verification and password resets need outbound mail. Set
`BASERUNNA_MAIL_WEBHOOK_URL` to an endpoint that accepts
`POST {"from", "to", "subject", "text"}` (optionally with
`BASERUNNA_MAIL_WEBHOOK_TOKEN` as a bearer token, and
`BASERUNNA_MAIL_FROM` as the sender): a small relay to SMTP or a mail
provider's HTTP API. Links point at `BASERUNNA_PUBLIC_URL`. Without a
webhook, the reset and verification endpoints answer 404, and Google
sign-in cannot link to existing password accounts (their addresses cannot
be verified). Mail is sent in the background, a few retries apiece; a node
that stops first drops it, and the user asks again.

## Shutdown

On SIGTERM (or SIGINT) a node:

1. fails its health check for `BASERUNNA_DRAIN_DELAY_MS` (default 0) while
   still serving, so the load balancer stops sending it requests;
2. stops accepting connections and lets in-flight requests finish, for at
   most `BASERUNNA_SHUTDOWN_TIMEOUT_MS` (default 20 s);
3. leaves the membership table and releases its database leases so that other
   nodes can take over writes immediately.

Set the drain delay to at least the load balancer's health check interval
times its unhealthy threshold, and give the orchestrator a grace period
longer than both settings together (on Kubernetes,
`terminationGracePeriodSeconds`). Interactive transactions open on a
stopping node are lost (`404 txn_not_found`, nothing committed). A killed
node loses nothing either; its databases just wait out their leases
(SPEC §5.7).

## Upgrades

If a new release changes the storage format, older nodes cannot read manifests
written by newer nodes. To upgrade a cluster across a format change, stop all
nodes before starting the new version.

## Bucket

### Requirements

The store must support conditional writes (`If-Match`, `If-None-Match`),
strongly consistent reads after writes, and ranged GETs: S3, GCS, Azure
Blob Storage, R2 and MinIO do. Use one bucket (or one prefix,
`s3://bucket/prefix`) per cluster, and nothing else should write there.

Credentials come from each provider's standard environment (`AWS_*`, for
S3-compatible stores also `AWS_ENDPOINT`; `GOOGLE_*`; `AZURE_*`), as read
by the `object_store` crate.

### Permissions

Nodes read, write, delete and list objects under the cluster's prefix, and
nothing else.

S3 (IAM policy for a prefix `baserunna/` of `my-bucket`):

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

GCS: `roles/storage.objectUser` on the bucket. Azure: `Storage Blob Data
Contributor` on the container.

### Lifecycle rules and versioning

Garbage collection is Baserunna's job: it deletes replaced segments,
unreferenced blobs and purged databases itself, after grace periods that
protect running reads (SPEC §5.8).

- **No expiration rules** on the cluster's prefix. Expiring an object that
  a manifest references loses data.
- **Abort incomplete multipart uploads** after a day. Baserunna writes
  every object with a single PUT (blobs are stored as 8 MiB chunks), so it
  leaves none behind; the rule costs nothing and cleans up after other
  tools.
- **Versioning off.** Every commit rewrites a database's manifest, so a
  versioned bucket keeps a noncurrent version per commit. If versioning
  must stay on, expire noncurrent versions after a day.
- Keep the bucket's storage class one with immediate reads (no archive
  tiers): any object may be read at any time.

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

Rate limits are enforced per node, so an N-node cluster allows up to N times the configured rate. Setting a limit to 0 disables it.

| variable                                | default | limits                                       |
| --------------------------------------- | ------- | -------------------------------------------- |
| `BASERUNNA_DB_MAX_MB`                   | 10240   | size of each database (segments and blobs)   |
| `BASERUNNA_USER_RPS`                    | 500     | requests per second per user                 |
| `BASERUNNA_API_KEY_RPS`                 | 500     | requests per second per API key (or its own) |
| `BASERUNNA_AUTH_RPS_PER_CLIENT`         | 1       | sign-ins and sign-ups per client address     |
| `BASERUNNA_SIGNUPS_PER_HOUR_PER_CLIENT` | 20      | accounts created per client address          |
| `BASERUNNA_HASH_CONCURRENCY`            | CPUs    | password hashes computed at once             |

## Configuration

Each node is configured through environment variables. The main ones are below; [`SPEC.md`](SPEC.md) §12 contains the full list:

| variable                                 | default                   |                                                                     |
| ---------------------------------------- | ------------------------- | ------------------------------------------------------------------- |
| `BASERUNNA_STORE`                        | `fs:./data`               | `fs:<dir>`, `mem:`, `s3://bucket/prefix`, `gs://…`, `az://…`        |
| `BASERUNNA_LISTEN`                       | `0.0.0.0:8080`            |                                                                     |
| `BASERUNNA_ADVERTISE`                    | `http://127.0.0.1:<port>` | URL that peers use to reach this node                               |
| `BASERUNNA_PUBLIC_URL`                   | advertise URL             | browser-facing URL (OAuth redirect, mailed links, `Secure` cookies) |
| `BASERUNNA_CLUSTER_SECRET`               | none                      | protects internal endpoints in multi-node clusters                  |
| `BASERUNNA_SIGNUP`                       | `first_user_only`         | `open`, `first_user_only`, or `closed`                              |
| `BASERUNNA_ADMIN_EMAIL` / `_PASSWORD`    | none                      | bootstrap an admin account                                          |
| `BASERUNNA_GOOGLE_CLIENT_ID` / `_SECRET` | none                      | enables "Sign in with Google"                                       |
| `BASERUNNA_MAIL_WEBHOOK_URL`             | none                      | enables verification and password reset mail                        |
| `BASERUNNA_LOG`                          | `info`                    | `error` / `warn` / `info` (logs every request) / `debug` / `off`    |
