# Intro

Baserunna is a web service that hosts small databases on object storage.

- Serializable transactions with strict serializability and snapshot isolation.
- Key Value API
- Large binary blobs stored out of band as chunked objects.
- Tables with schemas, primary keys, indexes, and schemas.
- Streams: append-only logs with retention and consumers.
- Stateless nodes: kill any node at any time.
- Verification with deterministic simulation testing

Design details are in [`SPEC.md`](SPEC.md).

## Be Bold

Treat every request as though you own the whole codebase and can make any change, anywhere.
There is zero legacy code to maintain, so every task MUST be approached from first principles.
If you see an approach that is subpar, prefer to rip it out wholesale and replace it with the better alternative.

## Comment Guidelines

- Doc comments should explain an API for the benefit of the caller.
- Code comments should be used only to explain unintuitive code. Whenever you add a code comment,
  you should ask yourself whether the comment is reflective of poorly written code or whether it
  is an expression of code that is so intrinsically complex that a comment is the only way to make
  the code understandable.
- Avoid changelog-style comments at all cost. They pollute the codebase and harm the reader's
  understanding of the code as it exists.

## Doc Guidelines

- If you find redundancies or overly verbose phrases while reading through a doc, remove them.
- LLM written docs often turn into factslop over time. If you find that a doc seems to contain
  a lot of facts but fails to communicate the key intent of a paragraph or section within the first
  sentence, restructure it to explain exactly what you were looking for, concisely.
- Never use middle voice. Instead of "writes commit together", use "writes are committed together".
- Never write factslop. by default, docs should focus on the concrete being solved over lists of symbols and intricate implementation details that are already present in the code.
