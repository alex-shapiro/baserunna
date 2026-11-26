# Playbook: Feature Development

Your job is to improve the product. A feature request is a hypothesis about how to improve it. Examine the hypothesis before you build anything.

Use judgment first. Use measurement only to support judgment. The easiest factor to measure rarely matters most. Do not choose a focus area, a candidate, or a winner because you can measure it.

## 1. Decide Whether to Build It

Create `BRIEF.md` at the repository root before you write code. Answer these questions in plain words:

- **Problem:** Who has the problem? What do they do today? What does that cost them?
- **Necessity:** What happens if we do nothing? Is this the most important problem in this area?
- **Request:** Is the request the real problem, or a solution for a different problem?
- **Precedent:** How do the best comparable systems solve this problem? If our design differs, why is our design better?
- **Cost:** What does the change cost? Check these areas:
  - New concepts that users must learn
  - New options
  - New failure modes
  - Weaker guarantees
  - Code to maintain
- **Size:** What is the smallest change that solves most of the problem? Estimate its size in lines of code.
- **Verdict:** Choose a verdict: build it, build less, build something different, or do not build it. State your reason.

Show the brief to the user. Stop and wait for the user if the verdict is not "build it". Also stop if the design depends on a product choice. Otherwise, continue.

## 2. Choose the Design

List approaches that use different designs. Treat different constants for the same approach as a single candidate. Always consider these approaches:

- Do less, or do nothing.
- Change the interface or the specification instead of the implementation.
- Delete a mechanism instead of adding a new mechanism.
- Move the problem to a different layer: the client, the store, or the operating system.
- Use the standard design from the best comparable system.

Judge each approach with these questions, in this order:

1. Does it solve the problem in the brief for a real user?
2. Can you explain its behavior to a user in two sentences? Will the behavior surprise them?
3. Does it add concepts and mechanisms, or remove them?
4. Would an expert in the field call it the standard design? If not, what makes ours better?
5. How does it fail? What does the user see if it fails?

If you can settle the choice by reasoning, decide without a contest. Write your argument in `BRIEF.md`.

### Contests

Run a contest only if the choice depends on a question that requires measurement. Examples are speed, memory use, and behavior under faults. Base each contest criterion on a user need in `BRIEF.md`. Set each cost limit as an absolute amount that a user notices, such as "adds at most 1 ms to a read". Do not use a percentage. A percentage of a small number can look large. For example, 0.09 ms to 0.14 ms is 54% slower, but users cannot notice 0.05 ms.

A contest is fair if every candidate can win:

- Before you build, record the result that makes each candidate win. If you cannot describe a result in which your favorite loses, you have already chosen.
- Build each candidate as its strongest advocate would, preferably in a separate sub-agent. Use a different sub-agent as the judge.
- Do not drop a candidate, especially the simplest candidate, because of an unmeasured cost.

Use the winner and continue. In the commit message, record the criteria, the results, and why each other candidate lost.

## 3. Build

Back every correctness claim with a realistic test. Write realistic tests that follow two rules:

- **Real inputs.** Use only client requests and production faults. Explain how each fault can happen in production.
- **Real states.** Reach each state through a sequence of real inputs. Do not construct state by hand, set internal fields, or call internal functions with arguments that the system never passes.

An unrealistic test can help you debug. It does not prove that a bug exists or that a fix matters. If you cannot trigger a failure with a realistic test, do not fix the failure. Report the failure as unproven.

Use property, fuzz, simulation, or load tests where they fit. Improve the test infrastructure if necessary.

Keep the change in proportion to its value:

- Stop if the diff is more than twice the size estimate in `BRIEF.md`. Decide whether the design is wrong before you continue. Record your answer in `BRIEF.md`.
- If you keep finding bugs in a mechanism, ask whether to delete the mechanism before you add another fix.

Existing designs, interfaces, and tests describe the current system. See the Requirements section of `AGENTS.md`. If an existing approach limits the quality of the result, replace it. Prove with tests that the replacement is better. Make this replacement only if the task requires it. Otherwise, add the problem to `TODO.md`.

Add a problem to `TODO.md` only if a user or operator notices it, or if it breaks a goal in section 1 of `SPEC.md`.

## 4. Version Control

Do all feature work in a branch. Put each subtask and each fix in a separate commit. In each commit message, include a short description and evidence of the change, such as test, experiment, or measurement results. Push each commit.

## 5. Check the Result

Before the review, read `BRIEF.md` again and answer these questions:

- Does the result solve the problem in the brief?
- If you started again today, would you choose the same design?
- What can you delete without harming users?

Act on the answers. Then start the [review](review.md).
