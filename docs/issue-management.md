# Issue Management for Solo Dev + LLM Workflow

## Overview

A minimal issue management system for solo developers using LLMs. Focus on clarity over process.

## Labels

Use only when they add real value:

- `idea` - Rough thought that needs fleshing out before implementation

That's it. By default, issues are ready for work unless marked as `idea`.

## Writing Issues

### For Implementation
```markdown
## What needs to be done
[Clear description]

## Context (if needed)
- Related files: [e.g., /http/handlers.go]
- Dependencies: [e.g., Needs #45 first]

## Success criteria
- [ ] Specific outcome
- [ ] Tests pass
```

### For Ideas
Just write a title with the `idea` label. Add details when ready.

## Workflow

1. **Create issue** - Either detailed (ready to work) or just a title with `idea` label
2. **Refine ideas** - When ready, remove `idea` label and add details
3. **LLM implements** - Works on issue, creates PR with "Fixes #X"
4. **Merge** - Review and merge PR (issue auto-closes)

## Best Practices

### For Humans
- Write clear outcomes, not implementation details
- Reference specific files when relevant
- Use `idea` label for brain dumps

### For LLMs
- Skip issues with `idea` label
- One issue = one PR
- Always run `make validate` before creating PR
- Include "Fixes #X" in PR description


## Multi-Issue Projects

For larger features, create a tracking issue:

```markdown
# [Feature Name]

Brief description

## Tasks
- [ ] #45 - Component 1
- [ ] #46 - Component 2
- [ ] #47 - Integration

Check off as PRs merge.
```

## Example

```
1. Human: "Need pagination" → Creates issue #45

2. LLM: Works on #45 → Creates PR "Fixes #45"

3. Human: Merges → Issue auto-closes
```

Simple as that.
