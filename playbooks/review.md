# Playbook: Reviewing a Branch

Use a review to decide whether the branch improves the product. Apply only the few changes that matter most. A review requires judgment. It is not a search for defects. A long list of findings shows a poor review.

You own every subsystem that the branch touches. Reviewers give advice. You make the decisions.

## Priorities

Review at three levels, in this order of importance:

1. **Product:** Is the change necessary and wise? What can users and operators do after the change? What can surprise them?
2. **System design:** Is this the best design? How do the parts fit together? What can we remove?
3. **Code:** Is the code easy to read? Is it small?

A higher-level finding always has priority over a lower-level finding. A code finding matters only if the issue makes the code much harder to understand or change.

## The Brief

Start the review from `BRIEF.md`, created during the [feature playbook](feature.md). If `BRIEF.md` does not exist, create it first by answering the questions in the feature playbook.

## Reviewers

Run each reviewer in a sub-agent to keep the reviewer independent from the fixer. Each reviewer has a specific job and prompt below. Use each prompt exactly as written, and add these items:

- the base branch
- the list of findings that you rejected in earlier rounds, with the reason for each rejection

Run the design reviewer on every branch. Also run the correctness reviewer if the branch does either of these things:

- changes data storage, reads, commits, or deletes
- can affect a goal in section 1 of `SPEC.md`

Run both reviewers in parallel.

Reviewers do not report names, comment wording, or style. Gemini fixes those issues during the copy edit step.

### Design Reviewer Prompt

```
Review the design of this branch against origin/main. Read AGENTS.md,
SPEC.md, BRIEF.md, and TRADEOFFS.md if it exists. Do not edit files, commit,
or push. Give your final report within 20 minutes.

Your job is to judge the design. Do not look for bugs. Another reviewer
checks for bugs. Answer these questions, in this order:

1. Should this change exist? Make the strongest case against a merge. Then
   state whether that case wins.
2. Does the branch solve the problem in BRIEF.md for a real user? What will
   surprise a user?
3. Would an expert choose this design if they started from nothing and
   knew the planned work? Read each plan listed in BRIEF.md. If a better
   design exists, describe it and state what you can delete.
4. Is the cost in proportion to the value? Consider new concepts, options,
   failure modes, and lines of code.
5. What can we delete from the branch without harming users?

Start with a verdict: merge, merge after changes, redesign, or do not merge.
Then give at most five findings, most important first. For each finding,
include:

- the effect on users or maintainers
- the cause
- the best fix
- the estimated net change in lines of code
- whether the fix requires a product tradeoff

Report a finding only if a user or operator would notice it, or if it makes
the system much harder to understand. Do not report a finding on the
rejected list unless you have new evidence. If you find nothing important,
say so. An empty report is a good result.

End with the most important problem outside these questions.
```

### Correctness Reviewer Prompt

```
Look for correctness bugs in this branch against origin/main. Read
AGENTS.md, SPEC.md, BRIEF.md, and TRADEOFFS.md if it exists. Do not commit
or push. Give your final report within 20 minutes. Keep each command under
5 minutes.

Find behavior that breaks a goal in section 1 of SPEC.md. Also find
behavior that causes wrong results, lost data, or stuck operations. Include
faults in the object store. The rest of SPEC.md and the existing tests
describe the current design and can be wrong. A difference between the code
and SPEC.md is a bug only if it harms a user. Ignore unreachable issues,
missing defensive checks, and missing tests for behavior that users do not
rely on.

Prove each bug. A proof is a failing test, a failing simulation seed, or an
exact sequence of events with file:line for each step. Each proof must
follow the realistic test rules in playbooks/feature.md. Use only client
requests and production faults, and reach each state through those inputs.
Label a bug "significant" only if you have a failing realistic test or seed.
You can add tests in a scratch directory to prove a bug. Do not change
repository code.

Give at most five bugs, most important first. For each bug, include the
proof, the effect on users, and the cause. A bug is usually a symptom of an
underlying cause. Look for other instances of the cause, and report the
cause. Do not report a bug on the rejected list unless you have new
evidence. If you do not find any bugs, say so. An empty report is a good
result.

End with the most important problem outside these instructions.
```

## Triage

Treat every finding as a claim that requires proof. Reject findings by default. Accept a finding only if all of these conditions are true:

1. **It is real.** For a bug, you reproduced it with a realistic test from the feature playbook. For a design finding, you checked the argument against the code.
2. **It matters.** A user or operator notices the problem in normal use, or it breaks a goal in section 1 of `SPEC.md`. A finding also matters if its fix simplifies the system.
3. **The fix is in proportion.** The fix costs less than the problem. Prefer fixes that delete code. If the fix adds a mechanism to repair a different mechanism, ask whether the first mechanism should exist.

A "redesign" or "do not merge" verdict has the highest priority. If you agree, stop all fixes. Return to step 2 of the feature playbook and add the new design as a candidate. If you disagree, write your reason in `BRIEF.md`.

Record each decision on a single line in `BRIEF.md`. Include the finding, whether you accepted or rejected it, and your reason. If you reject a significant finding because it is outside the scope of the branch, add it to `TODO.md`. Do not record other rejected findings in `TODO.md`.

## Fixing

Commit each fix separately. In each commit message, state the problem, the fix, and the test or measurement results.

## Tradeoffs

Before you make a change that needs a product tradeoff, ask the user to decide. Write the problem, the options, and their tradeoffs in a short section of `TRADEOFFS.md`. Include only real product or system tradeoffs. Do not treat a break in backward compatibility as a tradeoff.

After the user decides, delete the section. Record the decision and its reason in the commit message.

## Review Loop

Run a second round only if you accepted a design change or a significant bug fix in the first round. Use the second round to review the branch after those fixes. Stop after three rounds, or if you do not accept any findings in a round.

At the end of the review, tell the user:

- the verdict of the last design review
- each accepted finding and each rejected significant finding, with your reason
- the net change in lines of code from the review fixes
- the open tradeoffs

Then move the open tradeoffs to `TODO.md` and delete `TRADEOFFS.md`.

Work autonomously until the task is complete.

## Copy Editing

Claude Opus 5.5 is the strongest model for reasoning and coding. Gemini Argon 4 is the strongest model for writing. After you finish the review loop, use Gemini to copy-edit the branch and create the PR.

Check whether the environment provides the Antigravity CLI `agy`. The CLI is available only on a local machine. If `agy` is not available, tell the user and skip this step.

Run Gemini with this exact command:

```
agy --dangerously-skip-permissions -p "Run playbooks/copyedit.md on this branch."
```

After Gemini finishes, review its full diff. Make sure that formatting, linting, and tests pass. DO NOT EDIT ITS COPY CHANGES except to correct factual errors. If you edit copy to correct a factual error, run the Gemini command again.

If formatting, linting, or tests fail after Gemini runs, make the smallest edit that fixes the failure. Treat that edit as a factual fix. Run Gemini again at most two times. If errors remain after that, tell the user. After all checks pass, commit all changes and push all commits.
