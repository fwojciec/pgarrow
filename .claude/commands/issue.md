---
description: Start work on issue #$ARGUMENTS
---

Start work on issue #$ARGUMENTS:

1. Create branch `issue-$ARGUMENTS` from latest main
2. Review issue details with `gh issue view $ARGUMENTS`
3. Find relevant code and patterns
4. Track tasks with TodoWrite
5. Follow TDD if specified in project docs

Validate changes with `make validate` before committing.