# Release Process

This document describes how to publish a new release of agentexec.

## Overview

Creating a **GitHub Release** triggers three publish workflows automatically:

| Workflow | Target | Secret Required |
|---|---|---|
| `publish.yml` | PyPI | `PYPI_API_TOKEN` |
| `docker-publish.yml` | ghcr.io (`agentexec-worker`) | `GITHUB_TOKEN` (built-in) |
| `npm-publish.yml` | npm (`agentexec-ui`) | `NPM_TOKEN` |

Docker and npm publishing can also be triggered manually via workflow dispatch.

## Steps

### 1. Ensure CI is green

Push all changes to `main` and verify the CI workflow passes (unit tests on
Python 3.12/3.13 and Kafka integration tests).

### 2. Bump the version

Update the version in `pyproject.toml`:

```toml
version = "X.Y.Z"      # stable release
version = "X.Y.ZrcN"   # release candidate
```

For the UI package, also update `ui/package.json`.

Commit and push:

```bash
git add pyproject.toml
git commit -m "Bump version to X.Y.Z"
git push
```

### 3. Update CHANGELOG.md

Add a new section at the top of `CHANGELOG.md` with release notes covering
breaking changes, new features, improvements, bug fixes, and testing updates.

### 4. Tag the release

```bash
git tag vX.Y.Z
git push origin vX.Y.Z
```

### 5. Create the GitHub Release

This is what triggers the publish workflows.

```bash
gh release create vX.Y.Z --title "vX.Y.Z" --notes-file CHANGELOG.md
```

For release candidates, mark as a pre-release:

```bash
gh release create vX.Y.ZrcN --title "vX.Y.ZrcN" --generate-notes --prerelease
```

### 6. Verify

Check that all three publish workflows succeeded:

```bash
gh run list --limit 5
```

- **PyPI**: https://pypi.org/project/agentexec/
- **Docker**: https://ghcr.io/agent-ci/agentexec-worker
- **npm**: https://www.npmjs.com/package/agentexec-ui

## Manual Publishing

Docker and npm workflows support manual dispatch for one-off builds:

```bash
# Docker with a custom tag
gh workflow run docker-publish.yml -f tag=dev

# npm with a version override
gh workflow run npm-publish.yml -f version=0.2.0-beta.1
```
