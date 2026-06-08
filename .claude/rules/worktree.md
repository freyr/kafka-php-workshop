# Worktree Workflow

## Location and naming

Create worktrees in the .claude/worktrees/ directory of the source repo. Folder name is `<branch-slug>`:
- branch name as the rest, slugified: lowercase, `/` and `_` replaced with `-`, non-alphanumeric collapsed to `-`

Example: branch `feature/r15-builder-mother` → `.claude/worktrees/feature-r15-builder-mother`.

## Mandatory post-create steps

A fresh worktree has no `vendor/` — `git worktree add` only checks out tracked files, and `vendor/` is gitignored. Install composer dependency first

```bash
docker compose run --rm php composer install
```

## Never cross-link vendor

Do not symlink, bind-mount, or copy `vendor/` from another checkout. Always `composer install` fresh inside the worktree

## Cleanup after merge

From the origin checkout (not from inside the worktree):

```bash
git worktree remove <worktree-path>
git fetch --prune
```
