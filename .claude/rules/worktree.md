# Worktree Workflow
Prefer to always work within Worktree, use EnterWorktree tool to create and manage worktrees

## Mandatory post-create steps

A fresh worktree has no `vendor/` — `git worktree add` only checks out tracked files, and `vendor/` is gitignored. Install composer dependency first

```bash
docker compose run --rm php composer install
```

## Never cross-link vendor

Do not symlink, bind-mount, or copy `vendor/` from another checkout. Always `composer install` fresh inside the worktree

## Cleanup after merge

Use EnterWorktree and other build-ion tools to manage Worktrees lifecycle