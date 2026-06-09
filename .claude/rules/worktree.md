# Worktree Workflow

Always work inside a worktree. Never edit the main checkout directly — create
a worktree first, do the work there, and remove it once the branch is merged.

## Tools

Manage the worktree lifecycle with the built-in tools, not raw `git worktree`
commands:

- **`EnterWorktree`** — create a new worktree (on a new branch) and switch the
  session into it. Branch from the current local HEAD. The worktree is created
  under `.claude/worktrees/<name>/`. It only checks out tracked files, so
  `vendor/` will be missing (see post-create steps below). Pass `path` instead
  of `name` to switch into an existing worktree rather than create one.
- **`ExitWorktree`** — leave the worktree. Use `action: remove` to delete it,
  or `action: keep` to leave it on disk. `remove` only works on worktrees this
  session created; a worktree entered via `path` can only be kept.

## Mandatory post-create steps

A fresh worktree has no `vendor/` — `git worktree add` only checks out tracked
files, and `vendor/` is gitignored. Install composer dependencies first:

```bash
docker compose run --rm php composer install
```

## Never cross-link vendor

Do not symlink, bind-mount, or copy `vendor/` from another checkout. Always
`composer install` fresh inside the worktree.

## Cleanup after merge

Once the branch is merged, remove the worktree with `ExitWorktree`
(`action: remove`). This is a behavioral step — nothing fires automatically on
merge, so remove the worktree as soon as you observe the branch has landed.
