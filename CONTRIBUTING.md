# Contributing to streamLens

Thanks for your interest in contributing! This is a personal open-source project, and contributions of all kinds are welcome — bug reports, feature requests, documentation improvements, or code.

## Getting Started

1. **Fork** the repository and clone your fork locally.
2. Create a branch for your change: `git checkout -b my-feature`
3. Set up your local development environment (see the [README](README.md) for setup instructions).
4. Make your changes, test them, and commit.
5. Push to your fork and open a pull request.

## Reporting Issues

Before opening an issue, check if a similar one already exists. When creating a new issue:

- **Bugs** — Describe how to reproduce it, what you expected, and what actually happened. Screenshots or logs are helpful.
- **Feature requests** — Explain the problem it solves and why it would be useful.

## Making Changes

- Keep pull requests focused on a single change. Smaller PRs are easier to review and merge.
- Add or update tests if your change affects functionality.
- Make sure the app runs without errors before submitting.

### Project Structure

- `server/` — Python (FastAPI) backend
- `client/` — React + TypeScript frontend (Vite, Tailwind, shadcn/ui)
- `resources/` — Helper scripts for local development

### Local Development

```bash
# Backend
cd server
uv sync
uv run uvicorn main:app --reload --port 5000

# Frontend
cd client
npm install
npm run dev
```

## Commit Messages

We follow [Conventional Commits](https://www.conventionalcommits.org/) to keep the history clean and readable.

Format: `<type>: <short description>`

| Type | Use for |
|------|---------|
| `feat` | New features |
| `fix` | Bug fixes |
| `docs` | Documentation changes |
| `refactor` | Code restructuring without behavior change |
| `chore` | Build, tooling, or dependency updates |

**Tips:**
- Keep the first line under 72 characters.
- Use imperative mood: "Add feature" not "Added feature".
- If needed, add a blank line and a body explaining *why*, not *how*.

## Pull Requests

- Give your PR a clear title and description.
- Link to any related issues.
- Screenshots are appreciated for UI changes.

Once submitted, I'll review it as soon as I can. Don't hesitate to ask questions in the PR if anything is unclear.

## Code of Conduct

Be kind and respectful. This project follows the [Contributor Covenant](https://www.contributor-covenant.org/version/2/1/code_of_conduct/).
