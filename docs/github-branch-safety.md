# GitHub Branch Safety

GitHub reported that server-side branch protection and repository rulesets require GitHub Pro or a public repository for this private repo.

Current local safeguard:

- `.githooks/pre-push` blocks direct pushes to `main` from this checkout.
- `git config core.hooksPath .githooks` enables the hook locally.

Required server-side setup when available:

- protect `main`;
- require pull requests before merging;
- require CI/status checks before merging;
- block force pushes;
- block branch deletion;
- optionally require review approval if more than one maintainer is available.

