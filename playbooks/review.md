# Playbook: Reviewing a Branch

Bring this branch to a shippable state. You own every subsystem that the branch touches.

## Priorities

Review at three levels, in this order of importance:

1. **Product:** what application developers and operators can do, and what can surprise them.
2. **System design:** how the parts fit together.
3. **Code:** how readable and how small the code is.

Lower priorities have less weight, but remain in scope. Report findings at all three levels.

## Audit

### 1. Write the product brief

Before you read the code in detail, write a short brief:

- What new actions can a user take after this change?
- What can surprise a user?
- What promises do the docs make about the affected features?

Audit the branch and its dependent code paths against this brief.

### 2. Ask the questions

**Product**

- What does the change give users or take from them? Consider features, usability, reliability, safety, and speed.
- Does each affected feature work as a user expects? Does the code keep every promise in the brief?

**System design**

- Does each user action follow a single path through the code? Where do two paths do nearly the same thing? Find a design that merges them, even if the split predates the branch.
- Where must two pieces of code stay in sync? Prefer a design that removes the dependency or enforces it at compile time with shared types or macros.
- Where does the branch diverge from industry practice? A larger divergence requires a stronger justification.

**Code**

- Which types and signatures are hard to read because of deep nesting, large tuples, or many parameters? Find the requirement that forces the complexity, and simplify that requirement.
- What can we cut? Every line adds maintenance cost.

### 3. Report causes

A bug is usually a symptom of an underlying cause. For each finding, identify the cause and check for other instances of it. Report the cause.

Example:

- **Symptom:** "An IP literal skips the stage-2 check in `check_resolved`."
- **Cause:** "Network checks use two precedence orders. Stage 2 ignores tiers and ports, so the same connection can receive different decisions."

Fixing the symptom expands the second order. Fixing the cause removes it.

### 4. Look outside the questions

End each audit with a question: what is the most important problem outside this playbook?

## Fixing

Fix a problem if the change improves a priority level without degrading a higher priority level. Ensure that tests prove the new behavior.

Commit each fix separately. In the commit message, state the problem, the fix, and the test or measurement results so that future work can reference them.

## Tradeoffs

Before making a change that requires a product tradeoff, ask the user to decide. Document the problem, the options, and their tradeoffs in a short section of `TRADEOFFS.md`. Include only real product or system tradeoffs. Breaking backward compatibility is not a tradeoff.

After the user decides, remove the section and record the decision and rationale in the commit message.

If the branch relies on a significant preexisting problem that is out of scope to fix, add the problem to `TODO.md` and inform the user.

## Review Loop

Repeat the audit and fix cycle. Each audit covers the full branch and receives the prior fixes and `TRADEOFFS.md`. Stop if an audit finds only minor problems, or after five audits. Report any remaining problems to the user.

At the end of the review, report open tradeoffs to the user, move them to `TODO.md`, and delete `TRADEOFFS.md`.

Run each audit in a sub-agent to keep the reviewer independent from the fixer. Give each sub-agent full context. A restarted sub-agent with partial context makes poor design decisions. Start each reviewer with this prompt:

```
You own every subsystem that this branch touches. Run the audit in
playbooks/review.md on this branch against origin/main. Report what you
would change before you ship it. Read AGENTS.md and, if it exists,
TRADEOFFS.md. Do not edit files, commit, or push.

Start with the product brief. Report the findings from most to least important,
and label each finding "significant" or "minor". For each finding, give its
cause, the best fixes across the priorities, and whether each fix requires a
product tradeoff. Include file:line if the finding is at a single location. End
with the most important problem outside this playbook.
```

For a large branch, run three reviewers in parallel, one for each priority level. Append "Focus on the product", "Focus on the system design", or "Focus on the code" to their prompts. Combine their findings before applying fixes.

Run autonomously until the task is complete.

## Copy Editing

Claude Opus 5.5 is the strongest model for reasoning and coding. Gemini Argon 4 is the strongest model for writing. After the review loop completes, run Gemini for copy editing and PR creation.

Check whether the environment provides the Antigravity CLI `agy`. The CLI is available only on a local machine. If `agy` is unavailable, inform the user and skip this step.

Run Gemini with this exact command:

```
agy --dangerously-skip-permissions -p "Run playbooks/copyedit.md on this branch."
```

When Gemini is done, review its full diff and make sure that the codebase is formatted, linted, and tests pass. DO NOT EDIT ITS COPY CHANGES except to correct factual errors. If you have to edit copy in order to correct a factual error, re-run the Gemini command afterward.

If format, lint, or tests fail because of Gemini's changes, make the smallest edit that makes them pass, and treat it as a factual fix. Re-run Gemini at most two times. If errors remain after that, tell the user. When checks pass, commit anything uncommitted and push any unpushed commits.
