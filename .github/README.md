# GitHub Actions CI/CD

This repository uses GitHub Actions for continuous integration and deployment. The workflows are located in `.github/workflows/`.

## Workflows

### CI (`ci.yml`)
Runs on every push and pull request to main branches:
- **Lint**: Runs golangci-lint, go vet, and go fmt checks
- **Test**: Runs unit tests with coverage reporting
- **Build**: Compiles the binary and uploads as artifact
- **Docker Build**: Builds Docker image and runs security scanning

### E2E Tests (`e2e.yml`)
Runs end-to-end tests against a Kind Kubernetes cluster:
- Sets up Kind cluster
- Deploys the operator
- Runs e2e test suite

### Release (`release.yml`)
Triggered on version tags and releases:
- Builds and pushes multi-platform Docker images to GitHub Container Registry
- Generates installation YAML manifests
- Attaches artifacts to GitHub releases

### CodeQL Analysis (`codeql.yml`)
Runs security analysis:
- Scans Go code for security vulnerabilities
- Runs weekly and on PRs to main branches

## Setup Requirements

### Repository Secrets
No additional secrets are required. The workflows use `GITHUB_TOKEN` which is automatically provided.

### Container Registry
Images are pushed to GitHub Container Registry (`ghcr.io`) using the repository name.

### Branch Protection
Consider enabling branch protection rules for `main`/`master` branches requiring:
- Status checks to pass (CI workflow)
- Up-to-date branches
- Dismiss stale reviews

## Local Development

To run the same checks locally:

```bash
# Linting
make lint

# Unit tests
make test

# E2E tests (requires Kind)
make test-e2e

# Build
make build

# Docker build
make docker-build
```

## Coverage Reports

Test coverage reports are uploaded to Codecov. To view coverage:
1. Install the Codecov GitHub App
2. Visit the Codecov dashboard for your repository

## Security Scanning

The workflows include:
- Trivy vulnerability scanning for Docker images
- CodeQL analysis for source code
- Dependabot for dependency updates
