# Issue tracker

Track work and specs in GitHub Issues. Use `gh` with the repo's GitHub remote.

- Publish tickets with `gh issue create`. Use `--body-file` for multiline descriptions.
- Fetch tickets with `gh issue view <number> --comments`. Include labels when triaging.
- Use `gh issue list`, `gh issue comment`, `gh issue edit`, and `gh issue close` for tracker updates.

**PRs as a request surface: no.**

For Wayfinder, use a `wayfinder:map` issue with sub-issues for tickets. Record blockers as GitHub issue dependencies. Apply `ready-for-agent` when a ticket is ready to claim. Record the outcome on the ticket and link it from the map.
