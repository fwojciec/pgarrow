---
description: Address PR review comments thoughtfully
---

Address PR review comments:

1. View PR and comments with `gh pr view`
2. Evaluate each comment - don't blindly apply suggestions
3. For any bugs found: write a test that would have caught it FIRST
4. Consider context and implications before making changes
5. Track changes with TodoWrite
6. Run `make validate` after implementing
7. Document your decisions when responding

Key principle: When reviewers catch actual bugs, add regression tests immediately. These tests become institutional memory that prevents the issue from recurring.

Remember: AI suggestions may miss context. Think critically about whether changes improve the code or just make it different.
