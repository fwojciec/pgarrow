---
description: Address pull request comments with critical thinking and thorough investigation
---

You are helping to address comments on a pull request in the current repository. This command guides you through reviewing, investigating, and resolving PR feedback while maintaining code quality and documenting your decisions.

## Core Principles

When addressing PR comments, remember:

1. **Critical Thinking**: Every suggestion should be evaluated in context, not blindly applied
2. **Thorough Investigation**: Understand the full implications before making changes
3. **Documentation**: Record your reasoning for how you addressed each comment
4. **Quality Maintenance**: Ensure changes still pass all validation steps
5. **TDD for New Code**: Any new functionality must follow test-driven development practices

## Step 1: Identify the Pull Request

Use GitHub MCP tools to identify and analyze the current PR:

```
# Get current branch and find associated PR
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
    echo "WARNING: Cannot determine issue number from branch name"
    echo "Expected pattern: issue-<number>"
    echo "Current branch: $current_branch"
fi

# Get repository owner from git remote
repo_info=$(git remote get-url origin | sed 's/.*github.com[/:]\([^/]*\)\/\([^.]*\).*/\1\/\2/')
owner=$(echo $repo_info | cut -d'/' -f1)
repo=$(echo $repo_info | cut -d'/' -f2)

echo "Repository: $owner/$repo"
```

Then use the MCP tools to find the PR:
- `mcp__github__list_pull_requests` with head filter for current branch
- Extract PR number for subsequent operations

## Step 2: Fetch and Analyze Comments

Use GitHub MCP tools to retrieve comprehensive PR data:

**Primary Tools:**
- `mcp__github__get_pull_request_reviews` - Get all reviews and their states
- `mcp__github__get_pull_request_comments` - Get line-specific review comments
- `mcp__github__get_pull_request` - Get general PR information

**Fallback for Overview:**
```bash
# Use CLI for quick overview if MCP tools have issues
gh pr view $pr_number --comments
```

**Pay special attention to:**
- **GitHub Copilot comments**: Automated suggestions requiring contextual evaluation
- **Human reviewer feedback**: Requires careful consideration and discussion
- **CI/CD feedback**: Test failures or workflow issues
- **Security scanning alerts**: Critical issues that must be addressed

## Step 3: Categorize and Prioritize Comments

Organize feedback by urgency and type:

### **Critical Issues** (must fix):
- Security vulnerabilities
- Bugs or logic errors  
- Failing tests or CI/CD
- Violations of project standards (see CLAUDE.md)
- Breaking changes

### **Important Suggestions** (should address):
- Performance improvements
- Code clarity and maintainability
- Architecture alignment issues
- Missing test coverage
- Documentation gaps

### **Nice-to-Have** (consider addressing):
- Style preferences
- Minor optimizations
- Alternative implementation approaches
- Refactoring suggestions

## Step 4: Investigate Each Comment

For each comment, especially automated suggestions:

### 4.1 Understand the Context
- Use `Read` tool to examine surrounding code thoroughly
- Use `Grep` or `Task` tools to find related implementations
- Review original issue requirements and acceptance criteria
- Consider architectural patterns in CLAUDE.md and ai_context files
- Check for similar patterns elsewhere in the codebase

### 4.2 Evaluate the Suggestion
Critical thinking questions:
- Does this suggestion make sense in our specific context?
- What are the full implications across the codebase?
- Does it align with our project's patterns and standards?
- Could it introduce new issues or break existing functionality?
- Is the suggested approach consistent with our architecture?

### 4.3 Research When Uncertain
Use available tools to investigate:
- `WebSearch` for best practices and documentation
- `Grep` to find similar implementations in the codebase
- `Read` to examine related files and context
- Consider asking for clarification via PR comments

## Step 5: Plan Systematic Changes

For changes affecting multiple locations:

### 5.1 Scope Assessment
- Use `Grep` to find all affected files and locations
- Use `Task` tool for complex searches across the codebase
- Estimate the scope of changes required

### 5.2 Change Planning
- Use `TodoWrite` to track systematic updates and progress
- Plan changes in logical groups (interface → implementation → tests)
- Consider backwards compatibility and migration needs

### 5.3 Test-Driven Development for New Code
When comments require new functionality:
- **Write failing tests first** using testify library (assert/require)
- Follow project testing standards (see CLAUDE.md and ai_context/testing.md)
- Use `require` for setup that must succeed, `assert` for test conditions
- Ensure all new code paths have test coverage
- Run tests frequently during implementation

## Step 6: Implement Changes

### 6.1 Systematic Implementation
For widespread changes:
- Use `MultiEdit` with `replace_all` for consistent updates across files
- Group related changes into logical commits
- Update interface definitions before implementations
- Update mocks and tests to match new signatures

### 6.2 Implementation Guidelines
1. **Don't blindly apply suggestions** - Evaluate in full context
2. **Maintain project patterns** - Follow existing conventions
3. **Preserve functionality** - Ensure nothing breaks
4. **Add comprehensive tests** - Cover new functionality and edge cases
5. **Update documentation** - Reflect significant changes

### 6.3 Common Implementation Patterns
```bash
# For parameter order changes
1. Update interface definition first
2. Update all implementations
3. Update all mocks  
4. Update all test call sites
5. Run validation after each group

# For new functionality
1. Write failing tests first
2. Implement minimal code to pass tests
3. Refactor for quality
4. Add integration tests if needed
```

## Step 7: Handle Test Failures

### 7.1 Common Test Failure Patterns
- **Signature Changes**: Update mock implementations and all call sites
- **Parameter Order**: Use `Grep` to find all function calls systematically
- **Missing Mocks**: Add required mock services to test setup
- **Parallel Test Issues**: Check for shared state, use fresh mocks per test
- **Authentication Changes**: Update test auth setup

### 7.2 Test Recovery Process
1. Run `make validate` to identify all failures
2. Fix compilation errors first (signatures, imports)
3. Fix test logic errors (assertions, expectations)
4. Address race conditions in parallel tests
5. Re-run validation after each fix group

## Step 8: Validate Changes

### 8.1 Comprehensive Validation
Always run the full validation suite:
```bash
make validate
```

This includes: formatting, vetting, tests, module tidying, linting, and workflow validation.

### 8.2 Validation Failure Recovery
- Fix formatting issues first (`make format`)
- Address compilation errors before test failures
- Run specific test packages to isolate issues:
  ```bash
  go test ./postgres/...
  go test ./http/...
  ```
- For golden file tests, only update when necessary:
  ```bash
  go test ./scanner/... -update  # Only if you have API keys
  ```

## Step 9: Document Your Response

Use GitHub MCP tools to provide comprehensive feedback:

### 9.1 Add PR Comment
Use `mcp__github__add_issue_comment` for general PR feedback:

```markdown
## PR Feedback Addressed ✅

Thank you for the thorough review! I've addressed all comments as follows:

### **Critical Issues Fixed**
- **Security vulnerability in auth**: Implemented constant-time comparison (commit abc123)
- **Test failures**: Updated all mock signatures and test call sites (commit def456)

### **Code Quality Improvements**  
- **Parameter order inconsistency**: Standardized order across codebase to match SQL patterns (commit ghi789)
- **Missing test coverage**: Added comprehensive tests for new functionality following TDD approach (commit jkl012)

### **Architectural Alignment**
- **Copilot suggestion about error handling**: After investigation, found this conflicts with our error propagation pattern in ai_context/error-handling.md. Instead, improved error messages while maintaining standard pattern (commit mno345)

### **Not Addressed**
- **Style preference for variable naming**: Kept original naming to maintain consistency with existing codebase patterns

### **Validation Results**
- All tests pass with `make validate`
- New functionality has 100% test coverage
- All changes follow project TDD standards

**Commits**: Links to specific commits addressing each concern
```

### 9.2 Comment Types
- **General PR feedback**: Use `mcp__github__add_issue_comment`
- **Line-specific responses**: Use review comments if you have a pending review
- **Code suggestions**: Include commit references and reasoning

## Step 10: Finalize and Push

### 10.1 Final Validation
Run the complete checklist:
- [ ] Each change improves code without breaking functionality
- [ ] All suggestions evaluated in full context, not blindly applied
- [ ] Security implications thoroughly considered
- [ ] Performance impacts assessed
- [ ] Changes align with project patterns and standards
- [ ] Comprehensive tests cover all new functionality
- [ ] TDD approach followed for new code
- [ ] Documentation reflects significant changes
- [ ] `make validate` passes successfully
- [ ] All test failures resolved

### 10.2 Push Changes
```bash
# Push the changes
git push

# Verify PR is updated
mcp__github__get_pull_request to confirm changes are reflected
```

## GitHub MCP Tools Reference

### **Primary Tools for PR Comment Resolution:**

**PR Information:**
- `mcp__github__list_pull_requests` - Find PRs by branch or filters
- `mcp__github__get_pull_request` - Get detailed PR information
- `mcp__github__get_pull_request_files` - See changed files
- `mcp__github__get_pull_request_diff` - Get full diff

**Comments and Reviews:**
- `mcp__github__get_pull_request_comments` - Get line-specific review comments
- `mcp__github__get_pull_request_reviews` - Get all reviews and their states
- `mcp__github__add_issue_comment` - Add general PR comments
- `mcp__github__create_and_submit_pull_request_review` - Submit full reviews

**Repository Analysis:**
- `mcp__github__get_file_contents` - Read specific files
- `mcp__github__search_code` - Search across repository
- `mcp__github__list_commits` - Check recent changes

### **Error Handling:**
- **"No pending review found"** → Use `add_issue_comment` instead
- **"404 Not Found"** → Check repository owner/name with `git remote -v`
- **Rate limiting** → Space out API calls, use CLI as fallback

## Special Considerations for AI-Generated Comments

GitHub Copilot and other AI tools may:
- Miss important context about your specific application
- Suggest generic solutions that don't fit your architecture  
- Identify real issues but propose suboptimal fixes
- Flag false positives or edge cases

**Always:**
1. **Investigate the underlying concern** - Even if suggestion isn't perfect, issue might be real
2. **Consider broader context** - How does this fit your entire system?
3. **Apply your expertise** - You understand the codebase better than automated tools
4. **Document your reasoning** - Explain why you did or didn't follow suggestions
5. **Test thoroughly** - AI suggestions may not consider all test scenarios

## Success Criteria

Good PR comment resolution achieves:
- **Improved Code Quality**: Changes make the code better, not just different
- **Maintained Functionality**: All existing features continue to work
- **Comprehensive Testing**: New code has full test coverage following TDD
- **Clear Documentation**: Future developers understand the changes and reasoning
- **Project Alignment**: Changes fit the established architecture and patterns

Remember: The goal is thoughtful improvement, not blind compliance with suggestions.
