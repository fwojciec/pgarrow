---
description: Create a pull request following TDD principles and project standards
---

# Create Pull Request

You are helping to create a pull request in the current repository. This command guides you through a comprehensive PR preparation process that ensures code quality, maintains architectural consistency, and creates clear documentation of changes.

## Core Principles

Before we begin, remember these key principles that guide our PR process:

1. **TDD Adherence**: We write tests first, then implementation. The PR should demonstrate this approach.
2. **Architectural Consistency**: Changes must align with patterns in CLAUDE.md and ai_context files.
3. **Quality Gates**: Every PR must pass all validation steps before submission.
4. **Clear Communication**: The PR tells the story of why and how the change was made.

## Step 1: Pre-flight Checks

First, determine repository information and extract issue details from branch name:

```bash
# Get repository information from git remote
repo_url=$(git remote get-url origin)
if [[ "$repo_url" =~ github\.com[/:]([^/]+)/([^/.]+) ]]; then
    repo_owner="${BASH_REMATCH[1]}"
    repo_name="${BASH_REMATCH[2]}"
    echo "Repository: $repo_owner/$repo_name"
else
    echo "ERROR: Cannot determine repository owner/name from remote URL: $repo_url"
    echo "Expected GitHub URL format"
    exit 1
fi

# Export repo info for use in later steps
export repo_owner repo_name

# Check current branch
current_branch=$(git branch --show-current)
echo "Current branch: $current_branch"

# Extract issue number from branch name
if [[ "$current_branch" =~ ^issue-([0-9]+)$ ]]; then
    issue_number="${BASH_REMATCH[1]}"
    echo "Working on issue #$issue_number"
elif [[ "$current_branch" =~ ^(feature|fix|chore)/issue-([0-9]+) ]]; then
    # Backwards compatibility for old pattern
    issue_number="${BASH_REMATCH[2]}"
    echo "Working on issue #$issue_number (legacy branch pattern)"
else
    echo "ERROR: Cannot determine issue number from branch name"
    echo "Expected pattern: issue-<number>"
    echo "Legacy patterns also supported: feature/issue-<number>-<description>"
    echo "Current branch: $current_branch"
    exit 1
fi

# Export issue_number for use in later steps
export issue_number

# Check for uncommitted changes
if [[ -n $(git status --porcelain) ]]; then
    echo "Uncommitted changes detected:"
    git status --short
    echo ""
    echo "These may be implementation changes that should be committed as part of the PR."
    echo "Review the changes above and commit them if they represent your implementation."
    echo "If these are unrelated changes, stash them first: git stash"
    echo ""
    echo "Continuing with PR creation process..."
fi

# Ensure we're up to date with main (batch operations for efficiency)
git fetch origin main
if [[ $(git rev-list HEAD..origin/main --count) -gt 0 ]]; then
    echo "Branch is not up to date with origin/main"
    echo "Consider rebasing: git rebase origin/main"
fi
```

## Step 2: Verify Implementation Completeness

Review the implementation against the original issue requirements:

1. **Check Issue Details** (batch operations for efficiency):
   - Use `mcp__github__get_issue` with owner: $repo_owner, repo: $repo_name, issue_number: $issue_number
   - Review Success Criteria checklist
   - Ensure all Technical Requirements are addressed
   - Consider batching with other GitHub API calls (status checks, etc.)

2. **Verify TDD Approach**:
   - Confirm tests were written first (check git history)
   - Ensure all tests pass
   - Review test coverage for new code

3. **Architecture Alignment**:
   - Check CLAUDE.md for relevant patterns
   - Review ai_context files for architectural guidelines
   - Ensure new code follows established patterns

## Step 3: Run Comprehensive Validation

Execute validation (optimized single command approach):

```bash
# Run pre-commit hooks if they exist
if [[ -f .pre-commit-config.yaml ]]; then
    echo "Running pre-commit hooks..."
    pre-commit run --all-files
fi

# Comprehensive validation - this covers formatting, tests, linting, etc.
echo "Running comprehensive validation..."
make validate
if [[ $? -ne 0 ]]; then
    echo "ERROR: Validation failed. This would cause CI to fail."
    echo ""
    echo "Common fixes:"
    echo "- Formatting issues: run 'make fmt' and commit changes"
    echo "- Test failures: check with 'go test -v ./...'"
    echo "- Linting issues: review with 'make lint'"
    echo "- Module issues: run 'go mod tidy'"
    echo ""
    echo "Fix all validation issues before creating PR."
    exit 1
fi

# If formatting made changes, commit them
if [[ -n $(git status --porcelain) ]]; then
    echo "Validation applied formatting changes. Committing..."
    git add -A
    git commit -m "chore: apply code formatting

🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>"
fi
```

## Step 4: Prepare Commit History

Ensure commits tell a clear story:

```bash
# Review commit history
echo "Recent commits on this branch:"
git log --oneline main..HEAD

# Ask user if they want to clean up commit history
echo "Do you want to squash/reorder commits? (y/n)"
# If yes, guide through interactive rebase
```

## Step 5: Generate PR Description

Create a comprehensive PR description that includes:

1. **Issue Context**: Link to issue #$issue_number
2. **Problem Summary**: What problem does this solve?
3. **Solution Approach**: How was it implemented?
4. **TDD Process**: Confirm tests were written first
5. **Testing**: What tests were added/modified?
6. **Checklist**: 
   - [ ] Tests written first (TDD approach)
   - [ ] All tests passing
   - [ ] Code follows patterns in CLAUDE.md
   - [ ] `make validate` passes
   - [ ] Documentation updated if needed
   - [ ] Issue success criteria met

## Step 6: Create the Pull Request

Create PR using appropriate tool for environment:

```bash
# Determine PR title with appropriate conventional commit type
# feat(#123): for new features
# fix(#123): for bug fixes  
# docs(#123): for documentation
# refactor(#123): for refactoring
# chore(#123): for maintenance

# Option 1: GitHub CLI (if available)
if command -v gh &> /dev/null; then
    issue_title=$(gh issue view $issue_number --json title -q .title)
    gh pr create \
      --title "feat(#$issue_number): $issue_title" \
      --body "$pr_description" \
      --base main \
      --head "$current_branch" \
      --assignee @me
fi

# Option 2: MCP GitHub tools (preferred in Claude Code environment)
# IMPORTANT: Push the branch first before creating PR
# git push -u origin $(git branch --show-current)
#
# Then use mcp__github__create_pull_request with:
# - owner: $repo_owner
# - repo: $repo_name
# - title: "feat(#$issue_number): Issue Title"
# - head: current branch name
# - base: "main"
# - body: comprehensive PR description
```

## Step 7: Post-PR Actions

After creating the PR:

1. **Request Review**: 
   - Identify appropriate reviewers based on changed files
   - Request review via GitHub

2. **Monitor CI**:
   - Watch for CI pipeline results
   - Be ready to address any unexpected failures

3. **Update Issue**:
   - Use `mcp__github__add_issue_comment` with owner: $repo_owner, repo: $repo_name, issue_number: $issue_number
   - Include brief summary of implementation and link to PR
   - Move issue to "In Review" status if using project boards

## Reflection Questions

Before finalizing the PR, consider:

- Did you follow the TDD approach throughout?
- Does your implementation align with the architectural patterns?
- Is the PR focused and reviewable (not too large)?
- Will reviewers understand the context and decisions made?
- Have you validated that CI will pass?

## Tool Optimization for Claude Code Environment

When working in Claude Code with MCP tools, optimize for efficiency:

### Batch Operations
- Group related tool calls in single messages when possible
- Combine git status checks, fetches, and issue retrieval
- Use parallel tool execution for independent operations

### MCP-First Approach
- Prefer MCP GitHub tools (`mcp__github__*`) over CLI when available
- **ALWAYS** extract repository info first using `git remote get-url origin`
- Use `mcp__github__get_issue` with extracted owner/repo/issue_number
- Use `mcp__github__create_pull_request` with extracted owner/repo
- Use `mcp__github__add_issue_comment` with extracted owner/repo/issue_number

### Repository Information Extraction
When using MCP GitHub tools, follow this pattern:
1. **Extract repo info**: Use `git remote get-url origin` and parse owner/name
2. **Extract issue number**: Parse from branch name (issue-123 pattern)
3. **Use extracted values**: Pass to all MCP GitHub tool calls
4. **Never hardcode**: Repository names, issue numbers, or owner names

#### Example Implementation in Claude Code:
```
# Step 1: Extract repository information
git remote get-url origin  # Returns: https://github.com/owner/repo.git
# Parse to get: owner="owner", repo="repo"

git branch --show-current  # Returns: issue-59
# Parse to get: issue_number="59"

# Step 2: Use extracted values in MCP calls
mcp__github__get_issue with:
  - owner: "owner"       # ← Extracted, not hardcoded
  - repo: "repo"         # ← Extracted, not hardcoded  
  - issue_number: 59     # ← Extracted, not hardcoded

mcp__github__create_pull_request with:
  - owner: "owner"       # ← Same extracted values
  - repo: "repo"         # ← Same extracted values
  - title: "feat(#59): Issue Title"
  - head: "issue-59"
  - base: "main"
```

### Error Recovery
- Provide specific guidance for common validation failures
- Include recovery commands in error messages
- Guide through fixing rather than just erroring out

## Important Notes

- Never skip `make validate` - it's the final safety net before CI
- If validation fails at any step, fix issues before proceeding
- Keep PRs focused - if you've addressed multiple issues, consider separate PRs
- The PR description is documentation - make it comprehensive
- Use conventional commit types in PR titles for consistency
- Batch tool calls for better performance in MCP environments

Remember: A good PR is not just about the code changes, but about maintaining project quality, enabling effective review, and preserving the story of why and how changes were made.
