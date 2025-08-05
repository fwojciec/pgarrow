---
description: Create a pull request for the current branch
---

Create a pull request for the current branch:

1. Run `make validate` to ensure all checks pass
2. Commit any remaining changes
3. Push branch and create PR with `gh pr create`

The PR should:
- Reference the issue number (extracted from branch name)
- Describe what changed and why
- Confirm tests pass and validation succeeds
- Follow project patterns documented in CLAUDE.md