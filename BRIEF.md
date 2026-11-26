# Brief: Length limits on metadata command fields

## Problem

The lease holder for the system database runs every metadata command for every
tenant. The cost of a command grows with the length of its text fields. Today,
some fields that a client controls have no length limit:

- `GoogleLogin.sub` comes from the Google ID token. `verify_id_token` in
  `auth.rs` accepts a `sub` of any length.
- `RevokeSession.id`, `DeleteApiKey.id`, `SetDisabled.user_id`,
  `SetRole.user_id`, and `RemoveMember.user_id` come from URL path segments.
  The `http` crate accepts a URI of up to 65,534 bytes (`MAX_LEN` in
  `http-1.5.0/src/uri/mod.rs`).
- `CreateSession.client` comes from `X-Forwarded-For` when the node trusts
  proxy hops.

Other fields already have limits at the API layer. For example, emails are at
most 254 bytes, API key names at most 100 bytes, and database names at most 63
bytes.

A long field has two effects today (measured with a probe test on the table
layer):

- A lookup with a long ID makes the lease holder read and validate a key of up
  to 64 KiB. One request is cheap, but a tenant can repeat it, and the cost
  stays on the shared lease holder.
- A stored value over 4,160 bytes in an indexed column fails the commit with
  `500 internal: corrupt data: key too long`. A Google `sub` of that length
  gives the user a 500 error. It also puts a false "corrupt data" message in
  the operator's logs.

## Necessity

Milestone 2 of `ISOLATION_PLAN.md` needs a fixed cost limit for each metadata
command. A cost limit needs a size limit on each input. If we do nothing, the
next PR cannot test the "fixed number of rows and bytes" rule, because the byte
count has no limit.

## Request

The request is correct as written. The real problem is the cost of a command on
the lease holder. Field length is the input that controls the byte part of that
cost.

## Precedent

- OpenID Connect Core 1.0 §2 says that `sub` "MUST NOT exceed 255 ASCII
  characters in length".
- Stripe limits each metadata value to 500 characters. Firebase Auth limits a
  user ID to 128 characters. Both systems reject longer values with a client
  error. (Source: vendor documentation, from memory. I did not check it again
  for this brief.)

## Design

We considered four approaches:

1. Do nothing. This approach fails the milestone.
2. Add a check at each call site that builds a command. This approach has
   five new checks, and a new field can skip the checks.
3. Limit the encoded size of each command. This approach bounds the cost, but
   the error message cannot name the field.
4. Check every text field in one place, in `meta::apply`, on the lease holder.
   The check walks the encoded command. It rejects any text field longer than
   `META_FIELD_MAX` bytes and names the field. Numbers, booleans, and enums have
   a fixed size. No command has an array. Thus, the check covers every field,
   including fields that we add later.

We chose approach 4. It is one mechanism. The lease holder enforces it at run
time for local and forwarded commands. Users get a clear error:
`400 invalid_argument: the metadata field "id" is longer than 1024 bytes`.

We also enforce the OIDC rule in `verify_id_token`: `sub` must be 1 to 255
ASCII characters. This check gives a clear sign-in error at the correct layer.
It also rejects an empty `sub`, which would otherwise match any other account
with an empty `sub`.

`META_FIELD_MAX` is 1024 bytes. The longest valid field today is an email
address (254 bytes) or a node address from configuration.

## Cost

- No new concepts or options for users.
- One new failure: a text field over 1024 bytes in a metadata request returns
  `400`. No valid request has such a field.
- No weaker guarantees.
- About 40 lines of code and 60 lines of tests.

## Verdict

Build it.

## Related problem

The table layer accepts an indexed value over 4,160 bytes and then fails the
commit with a false "corrupt data" error. This problem affects user tables too.
It is in the storage domain, so this PR does not fix it. `TODO.md` lists it as
untriaged.

## Result check

- The result solves the problem. The lease holder rejects every text field
  over 1024 bytes in each metadata command, from any source.
- I would choose the same design again. The check costs one encode of the
  command, and the command is at most a few KiB.
- I cannot delete a part without harm. The `sub` check overlaps the field
  limit, but it also rejects an empty or non-ASCII `sub`, and it follows the
  OIDC rule.
- The diff is 63 lines. The estimate was about 100 lines.

## Review decisions
