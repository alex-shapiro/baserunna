# Playbook: Feature Development

Your job is to make the product better. A feature request is a hypothesis about how to do that. Examine the hypothesis before you build anything.

Judgment comes first in this playbook, and measurement serves judgment. The criterion that is easiest to measure is rarely the one that matters most. Never choose a focus area, a candidate, or a winner because it gives a number.

## 1. Decide Whether to Build It

Before you write code, write `BRIEF.md` at the repository root. Answer these questions in plain words:

- **Problem:** Who has the problem? What do they do today, and what does it cost them?
- **Necessity:** What happens if we do nothing? Is this the most important problem in this area?
- **Request:** Is the request the real problem, or a proposed solution to a different problem?
- **Precedent:** How do the best comparable systems solve this problem? If we plan to diverge from them, why is our way better?
- **Cost:** What does the change cost? Consider new concepts that users must learn, new knobs, new failure modes, weaker guarantees, and code to maintain.
- **Size:** What is the smallest change that solves most of the problem? Estimate its size in lines of code.
- **Verdict:** Build it, build less, build something different, or do not build it. Give the reason.

Show the brief to the user. If the verdict is anything other than "build it", or if the design depends on a product choice, stop and wait for the user to decide. Otherwise, continue without waiting.

## 2. Choose the Design

List approaches that differ in kind, not only in parameters. Different constants for one approach make one candidate, not several. Always consider these approaches:

- Do less, or do nothing.
- Change the interface or the spec instead of the implementation.
- Delete a mechanism instead of adding one.
- Move the problem to a different layer: the client, the store, or the operating system.
- Use the standard design from the best comparable system.

Judge each approach with these questions, in this order:

1. Does it solve the problem in the brief for a real user?
2. Can you explain its behavior to a user in two sentences, and will the behavior surprise them?
3. Does it add concepts and mechanisms, or remove them?
4. Would an expert in the field call it the standard design? If not, what makes ours better?
5. How does it fail, and what does the user see when it fails?

Decide by argument if reasoning can settle the choice. Write the argument in `BRIEF.md`.

### Contests

Run a contest only if the choice depends on an empirical question that you cannot answer by reasoning. Examples are speed, memory use, and behavior under faults. Each contest criterion must be a property that the brief says users care about. Set each cost limit as an absolute amount that a user would notice, such as "adds at most 1 ms to a read". Do not use a percentage. A percentage of a small number can look large: a change from 0.09 ms to 0.14 ms is 54% slower, but no user can see 0.05 ms.

A contest is fair if every candidate can win:

- Before you build, write down the result that would make each candidate the winner. If you cannot describe a result in which your favorite loses, you have already chosen.
- Build each candidate as its strongest advocate would, preferably in a separate sub-agent. Use a different sub-agent as the judge.
- Do not drop a candidate, especially the simplest one, because of a cost that you did not measure.

Adopt the winner and continue. In the commit message, record the criteria, the results, and the reason that each other candidate lost.

## 3. Build

Back every claim about correctness with a realistic test. A realistic test obeys two rules:

- **Real inputs.** The test drives the system only with requests that a client can send and with faults that can happen in a real deployment. For each fault, you must be able to say how it happens in production.
- **Real states.** The test reaches each state through a sequence of real inputs. It does not build state by hand, set internal fields, or call internal functions with arguments that the system never passes.

A test that breaks these rules can help you debug, but it does not prove that a bug exists or that a fix matters. If you cannot reach a failure with a realistic test, do not fix it. Report it as unproven.

Use property, fuzz, simulation, or load tests where they fit, and improve the test infrastructure if necessary.

Keep the change in proportion to its value:

- If the diff grows past twice the size estimate in the brief, stop. Decide whether the design is wrong before you continue. Update the brief with the answer.
- If tests keep finding bugs in a mechanism, ask whether the mechanism should exist before you patch it again.

Existing designs, interfaces, and tests are descriptive, not prescriptive (see the Requirements section of `AGENTS.md`). If an existing approach limits the quality of the result, replace it, and prove with tests that the replacement is better. Replace it only if the task needs the replacement. Otherwise, add the problem to `TODO.md`.

Add a problem to `TODO.md` only if a user or operator would notice it, or if it breaks a goal in section 1 of `SPEC.md`.

## 4. Version Control

Do all feature work in a branch. Put each subtask and each fix in its own commit. Each commit message gives a short description and the evidence of its effect: the tests, experiments, or measurements and their results. Push each commit.

## 5. Check the Result

Before the review, read the brief again and answer these questions:

- Does the result solve the problem in the brief?
- If you started again today, would you choose the same design?
- What can you delete with no loss to users?

Act on the answers, then begin the [review](review.md).
