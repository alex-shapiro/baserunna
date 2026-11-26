# Playbook: Reviewing a Branch

A review decides whether the branch makes the product better, and makes the few changes that matter most. A review is an act of judgment. It is not a search for defects. A long list of findings is a sign of a poor review.

You own every subsystem that the branch touches. Reviewers give advice. You make the decisions.

## Priorities

Review at three levels, in this order of importance:

1. **Product:** Is the change necessary and wise? What can users and operators do after the change, and what can surprise them?
2. **System design:** Is this the best design? How do the parts fit together, and what can we remove?
3. **Code:** Is the code easy to read, and is it small?

A finding at a lower level never outweighs a finding at a higher level. A code finding matters only if it makes the code materially harder to understand or to change.

## The Brief

The review starts from `BRIEF.md`, which the [feature playbook](feature.md) creates. If `BRIEF.md` does not exist, write it first with the questions in the feature playbook.

## Reviewers

Run each reviewer in a sub-agent, so that the reviewer is independent from the fixer. Each reviewer has a different job and gets its own prompt below. Use each prompt exactly as written, and add these items:

- the base branch
- the list of findings that you rejected in earlier rounds, with the reason for each rejection

Run the design reviewer on every branch. Also run the correctness reviewer if the branch changes how data is stored, read, committed, or deleted, or if it can affect a goal in section 1 of `SPEC.md`. Run the two reviewers in parallel.

No reviewer reports names, comment wording, or style. The copy edit step handles them.

### Design Reviewer Prompt

```
You review the design of this branch against origin/main. Read AGENTS.md,
SPEC.md, BRIEF.md, and TRADEOFFS.md if it exists. Do not edit files, commit,
or push. Give your final report within 20 minutes.

Your job is judgment, not defect search. Do not look for bugs; another
reviewer does that. Answer these questions, in this order:

1. Should this change exist? Make the strongest case against a merge. Then
   say whether that case wins.
2. Does the branch solve the problem in BRIEF.md for a real user? What will
   surprise a user?
3. Would an expert choose this design if they started from nothing? If a
   better design exists, describe it and say what it would delete.
4. Is the cost in proportion to the value? Consider new concepts, knobs,
   failure modes, and lines of code.
5. What can we delete from the branch with no loss to users?

Start with a verdict: merge, merge after changes, redesign, or do not merge.
Then give at most five findings, most important first. For each finding,
give the effect on users or maintainers, the cause, the best fix, and the
estimated net change in lines of code. Say if the fix needs a product
tradeoff.

Report a finding only if a user or operator would notice it, or if it makes
the system materially harder to understand. Do not report a finding that
appears in the list of rejected findings, unless you have new evidence. If
you find nothing important, say so. An empty report is a good result.

End with the most important problem outside these questions.
```

### Correctness Reviewer Prompt

```
You look for correctness bugs in this branch against origin/main. Read
AGENTS.md, SPEC.md, BRIEF.md, and TRADEOFFS.md if it exists. Do not commit
or push. Give your final report within 20 minutes. Keep each command under
5 minutes.

Find behavior that breaks a goal in section 1 of SPEC.md or that gives a
user wrong results, lost data, or a stuck operation. Include faults in the
object store. The rest of SPEC.md and the existing tests describe the
current design and can be wrong. A difference between the code and SPEC.md
is a bug only if it harms a user. Ignore theoretical issues that have no
reachable path, missing defensive checks, and missing tests for behavior
that no user depends on.

Prove each bug. A proof is a failing test, a failing simulation seed, or an
exact sequence of events with file:line for each step. The proof must obey
the rules for a realistic test in playbooks/feature.md: it uses only client
requests and production faults, and it reaches each state through those
inputs. Label a bug "significant" only if you have a failing realistic test
or seed. You may add tests in
a scratch directory to prove a bug, but do not change code in the
repository.

Give at most five bugs, most important first. For each bug, give the proof,
the effect on users, and the cause. A bug is usually a symptom of an
underlying cause. Look for other instances of the cause, and report the
cause. Do not report a bug that appears in the list of rejected findings,
unless you have new evidence. If you find no bugs, say so. An empty report
is a good result.

End with the most important problem outside these instructions.
```

## Triage

A finding is a claim, not an order. Reject findings by default. Accept a finding only if all of these conditions are true:

1. **It is real.** You reproduced the bug with a realistic test, as the feature playbook defines it, or you checked the design argument against the code yourself.
2. **It matters.** A user or operator would notice the problem in normal operation, or the problem breaks a goal in section 1 of `SPEC.md`, or the fix makes the system simpler.
3. **The fix is in proportion.** The fix costs less than the problem. Prefer fixes that delete code. If the fix adds a mechanism to repair a different mechanism, ask whether the first mechanism should exist.

A "redesign" or "do not merge" verdict has the highest priority. If you agree with it, stop all fixes and go back to step 2 of the feature playbook with the new design as a candidate. If you disagree, write your reason in `BRIEF.md`.

Record each decision as one line in `BRIEF.md`: the finding, "accepted" or "rejected", and the reason. If you reject a significant finding because it is outside the scope of the branch, add it to `TODO.md`. Do not file the other rejected findings.

## Fixing

Commit each fix separately. In the commit message, state the problem, the fix, and the results of the test or measurement, so that future work can use them.

## Tradeoffs

Before you make a change that needs a product tradeoff, ask the user to decide. Write the problem, the options, and their tradeoffs in a short section of `TRADEOFFS.md`. Include only real product or system tradeoffs. Breaking backward compatibility is not a tradeoff.

After the user decides, remove the section, and record the decision and its reason in the commit message.

## Review Loop

Run a second round only if the first round accepted a change to the design or a significant bug fix. The second round checks the branch after those fixes. Stop when a round has no accepted findings, or after three rounds.

At the end of the review, tell the user:

- the verdict of the last design review
- the accepted findings, and the rejected findings that were labeled significant, each with your reason
- the net change in lines of code that the review fixes caused
- the open tradeoffs

Then move the open tradeoffs to `TODO.md` and delete `TRADEOFFS.md`.

Run autonomously until the task is complete.

## Copy Editing

Claude Opus 5.5 is the strongest model for reasoning and coding. Gemini Argon 4 is the strongest model for writing. After the review loop is complete, use Gemini for copy editing and to create the PR.

Check whether the environment provides the Antigravity CLI `agy`. The CLI is available only on a local machine. If `agy` is not available, tell the user and skip this step.

Run Gemini with this exact command:

```
agy --dangerously-skip-permissions -p "Run playbooks/copyedit.md on this branch."
```

When Gemini is done, review its full diff. Make sure that the codebase is formatted and linted, and that the tests pass. DO NOT EDIT ITS COPY CHANGES except to correct factual errors. If you edit copy to correct a factual error, run the Gemini command again.

If format, lint, or tests fail because of Gemini's changes, make the smallest edit that makes them pass, and treat it as a factual fix. Run Gemini again at most two times. If errors remain after that, tell the user. When the checks pass, commit all changes and push all commits.
