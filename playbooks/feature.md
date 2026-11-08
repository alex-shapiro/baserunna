# Playbook: Feature Development

This playbook will help you build features that both satisfy product goals and produce high quality technical systems.

## Thinking

A feature request may be underspecified or confused about the technical approach to take. Thus, a large part of your job is to explore the question and design spaces fully. The **key criteria** to consider are mostly qualitative:

1. How does a change materially improve or degrade the user-facing product? More useful featureset, more usable interface, higher reliability, improved safety, faster performance, etc.
2. After a change, do all impacted features work as a normal user would expect?

In some cases, the request may be misguided: even the best solution would degrade the product or lead to unexpected behavior. In such a case, your overriding responsibility is not to implement the request, but to:

- determine the nature of their misguided request (e.g. is this an XY problem?)
- communicate with the user to iterate on the request

## Implementation

Think through all possible ways to attack the problem. If there are multiple plausible candidates, build the best possible, fully baked version of each and test them against each other in a fair contest. The winner is the candidate that best fulfills the key criteria.

A contest is fair if all candidates can win. Pre-register the result that would make each candidate the winner. If you cannot describe a result in which your favorite loses, you have already chosen, and the contest is only a formality. Build each candidate as its strongest advocate would, preferably in a separate sub-agent. Use a separate sub-agent as judge. In the commit message, record the criteria, the results, and the reason each losing candidate lost.

Ensure all claims are backed by solid evidence. Where useful, use property, fuzz, load, or other testing techniques. Make changes to testing infra as appropriate.

Existing interfaces and planning docs are descriptive, not prescriptive. Before presenting work, list and remove every limit they placed on your design. If you catch anything that should be fixed but is out of scope for the current task, add it to TODO.md so we can steadily improve quality over time.

Approach the full task, and all subtasks, from first principles. If you see that an existing approach limits the quality ceiling of the task, even in another repo, prefer to replace the subpar approach with the better alternative. If you do rip-and-replace, verify that the replacement is superior with rigorous testing.

## Version Control

All feature development must take place in a branch. Where appropriate, split subtasks, followups, and fixes into individual git commits. Each commit should contain a concise description, as well as evidence of impact. Include references to tests, experiments, or measurements and their outcomes, so follow-up work can reference past findings easily. Push each commit.

## Upon Completion

When the task is complete, begin a [review](review.md).
