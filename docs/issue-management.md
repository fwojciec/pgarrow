# Issue Management

## Writing Issues

```markdown
## Problem
[What needs to be fixed/added]

## Context  
- Related files: [e.g., /http/handlers.go]
- Dependencies: [e.g., Needs #45 first]

## Success Criteria
- [ ] Specific outcome
- [ ] Tests pass with `make validate`
```

## Workflow

1. **Create issue** - Clear problem statement with success criteria
2. **Implement** - Create branch `issue-X`, write tests, implement solution
3. **Validate** - Run `make validate` to ensure quality
4. **Create PR** - Include "Fixes #X" to auto-close issue
5. **Review & merge** - Address feedback, add regression tests for any bugs found

## Principles

- Write **what** needs doing, not **how**
- One issue = one PR
- Branch naming: `issue-X`
- Always validate before creating PR
- Write regression tests for bugs caught in review
- Reference specific files when helpful

## Multi-Issue Features

For larger features, create a tracking issue:

```markdown
# [Feature Name]

## Tasks
- [ ] #45 - Component 1
- [ ] #46 - Component 2  
- [ ] #47 - Integration
```

Check off as PRs merge.

## Example

```
Issue #45: "Add rate limiting to API"
→ Branch: issue-45
→ PR: "feat(#45): Add rate limiting to API" 
→ Merge: Issue auto-closes
```