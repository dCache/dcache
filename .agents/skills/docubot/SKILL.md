---
name: docubot
description: Inspects the most recent commit in a dCache repository and, when it changes user-facing behavior, drafts a documentation update to TheBook as a pull request. Trigger this after a commit lands on the default branch, or whenever asked to check whether a commit needs a docs update.
---

# Docubot Skill

Docubot reviews the most recent commit in the repository, decides whether it needs a matching update in TheBook (dCache's site-administrator documentation), and — if so — drafts that update and opens it as a pull request for a human to review.

Most commits (bug fixes, refactors, test changes, internal cleanup) don't need a documentation change, and that's the expected outcome. Only proceed past Phase 1 when there's a real reason to.

## Phase 1: Decide whether documentation is needed

Inspect the commit with `git show HEAD` (or `git show <commit>` if a specific commit is given) to see both the full message and the diff.

An update is warranted when the commit:

0. Explicit `Require-book: yes` in the commit message, but no documentation update. Event if the commit message says ``Require-book: no`, the update might still be required (see next points).
1. Changes user-facing behavior — a new configuration option or property, a changed default, a deprecated or removed feature, or a renamed component/property/command.
2. Introduces a new module, component, or protocol capability that a site administrator would need to know about.
3. Already touches files under `docs/`, but the change looks incomplete, unclear, or has grammar/spelling issues that would confuse a site administrator reading it.


## Phase 2: Draft the documentation update

Documentation lives under `docs/TheBook/src/main/markdown`.

- Find the existing page(s) most relevant to the change (search for the component, config property, or feature name). Prefer extending an existing page over adding a new one.
- If nothing fits, create a new page — and also add it to whatever table-of-contents/summary file the book uses, so it's actually reachable once built.
- Match the surrounding style: heading structure, terminology already used elsewhere in the book, and any existing callout/admonition conventions.
- Write for the target audience — dCache site administrators operating a running instance, not developers reading the source. Cover what changed operationally: how to configure it, what the new or changed behavior means for their deployment, and any migration notes if a default changed or something was renamed or removed.
- Keep the change focused on what the commit actually did; don't rewrite unrelated parts of the page while you're in there.

## Phase 3: Open the pull request

Before branching, confirm the working tree is clean (stash or abort otherwise, so the docs branch doesn't pick up unrelated changes), and check whether a branch or PR for this commit already exists (`git ls-remote --heads origin docubot-<githash>`) so repeated runs don't create duplicates.

1. Branch from the commit being evaluated:
   ```
   git checkout -b docubot-<githash>
   ```
   `<githash>` is the short hash of the commit from Phase 1.

2. Stage and commit only the documentation files you changed:
   ```
   git add docs/TheBook/...
   git commit --author="dCache Bot <dcache-ci@dcache.org>" \
     -m "[skip-ci] docs: <short summary of the change>" \
     -m "Assisted-by: <AI-model>/<AI-provider>"
   ```
   Fill in `<AI-model>/<AI-provider>` with whatever actually produced the suggestion, e.g. `reasoning/desy-assistant`.
   Use `[skip-ci]` prefix for commit message to bypass CI for documentation only changes.

3. Push the branch:
   ```
   git push -u origin docubot-<githash>
   ```

4. Open the PR:
   ```
   gh pr create --base <default-branch> --head docubot-<githash> \
     --title "[skip-ci] docs: <short summary>" \
     --body "Suggested documentation update for <githash> (\"<commit subject>\"). AI-generated — please review for accuracy before merging."
   ```

5. Flag it for reviewers with an `eyes` reaction:
   ```
   gh api repos/<owner>/<repo>/issues/<pr-number>/reactions -f content=eyes
   ```
   `<pr-number>` is printed by `gh pr create`, or can be read back with `gh pr view --json number`.

Leave the PR open for a human to review — never merge it automatically. The suggestion is a starting point, not a finished change.
