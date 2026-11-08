# Intro

Baserunna is a web service that hosts small databases on object storage.

- Serializable transactions with strict serializability and snapshot isolation.
- Key Value API
- Large binary blobs stored out of band as chunked objects.
- Tables with schemas, primary keys, indexes, and schemas.
- Streams: append-only logs with retention and consumers.
- Event tables: columnar, time-partitioned analytics with funnels, retention and unique counts.
- Stateless nodes: kill any node at any time.
- Verification with deterministic simulation testing

Design details are in [`SPEC.md`](SPEC.md).

## Playbooks

- Follow playbooks/feature.md for a feature request.
- Follow playbooks/review.md for a system or code review request.
- Follow playbooks/copyedit.md for a copy editing request.

If playbook instructions conflict with this doc, follow the playbook and communicate the conflict to the user.

## Be Bold

Approach each task from first principles. This is a greenfield codebase with no legacy code to maintain. You own the whole codebase and can make any change, anywhere.

## Symbol Names

Symbol names must be idiomatic and just specific enough that a reader can

- understand it without requiring surrounding context
- distinguish it from other symbols with similar names but different uses
- connect it to its related symbols (example: a getter and its return type are related)

| bad                  | good                      |
| -------------------- | ------------------------- |
| ReadId               | StdinReadId               |
| Who                  | DocOwner                  |
| wanted               | has_waiting_readers       |
| to                   | target                    |
| until(...) -> Future | wait_until(...) -> Future |
| wait() -> WaitKind   | wait_kind() -> WaitKind   |

## Comment Guidelines

A symbol's doc comment, and especially its first line, must reflect the symbol's purpose. For example, if a type represents an HTTP route, the type's doc comment should be its HTTP route because its purpose is to serve that route. Avoid using a doc comment to elaborate on implementation details that the caller does not care about. If such implementation details are actually necessary because of non-obvious code, flag it to the user.

Before writing a code comment, ask yourself whether the comment is reflective of poorly written code. Is it an expression of code that is so intrinsically complex that the extra writing is the only way to make the code understandable? Typically, there is a better way to make the code or interface understandable. Exceptions to this rule tend to be interfaces that _do_ have inherent complexity, like a regex library or policy file format.

Avoid changelog-style comments at all cost. They pollute the codebase and harm the reader's understanding of the code as it exists.

## Writing and User Communication Guidelines

Every output that is not code, including docs, comments, and communication with the user, must be written in AST-STE100 English. Convey key ideas in leading sentences to ensure they are easy to find. Avoid runs of facts and symbols, which are impossible for a human to parse. If you generate a run of facts and symbols, step back and consider how to apply AST-STE100 guidelines to structure output more effectively. See the copy editing playbook for detailed guidelines.

Remove all redundancies and overly verbose phrases you find while reading.

Never write with middle voice. If you read something that already contains middle voice, rewrite it to follow AST-STE100 guidelines.

Communication Anti-patterns that Claude, in particular, must avoid:

- Writing carefully, for self-protection, instead of clearly. This leads to legalese, jargon, hedges, and qualifiers.
- Swinging between too little context and unnecessary padding in answers.
- Failing to provide context when the user needs to make a decision.
- Adding questions and irrelevant options when the user asks a simple question.
- Fixating on a narrow interpretation and ignoring a larger truth.

### Teaching

When communicating with a human user, the user may ask a question in a way that suggests they do not understand a concept. When this occurs, it is your job to teach them. The user is more likely to have 5% context than 99% context, and teaching a concept to a person who does not understand it is very different than explaining a concept to a fellow LLM with the same context and ability to follow reference shorthand. A human must be taught incrementally. Walk the user through one idea at a time, one sentence at a time, and pause to let them think, ask questions, and follow up. Avoid repeated offers to "Let me know when you want to follow up." Say it at most once and then let the user do it themselves.
