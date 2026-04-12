# Contributing to StorageGrid Operator

This guide covers the development workflow, tooling, and testing setup for the StorageGrid Operator.

## Prerequisites

| Tool    | Version | Purpose                      |
| ------- | ------- | ---------------------------- |
| Go      | 1.25+   | Build and test               |
| Docker  | —       | Container image builds       |
| kubectl | —       | Cluster interaction          |
| yq      | 4.x     | Chainsaw values sanitization |
| Make    | —       | Build automation             |

Most development tools (controller-gen, kustomize, golangci-lint, chainsaw, etc.) are downloaded automatically into `bin/` by Make targets.

## Project Structure

```text
api/v1alpha1/       # CRD type definitions (StorageGrid, S3Tenant, S3Bucket, etc.)
cmd/                # Operator entrypoint
internal/
  controller/       # Reconciliation logic for all CRDs
  webhook/          # Admission webhooks
pkg/
  grid/             # StorageGrid API client and business logic
  kube/             # Reusable Kubernetes utility functions
  s3/               # S3 client operations
config/             # Kustomize manifests (CRDs, RBAC, deployment, webhooks)
docs/architecture/  # Architecture docs (controller patterns, separation of concerns, etc.)
test/e2e/           # End-to-end tests (Kind-based and Chainsaw)
hack/               # Development scripts
```

See [docs/architecture/](docs/architecture/) for detailed design documentation.

## Quick Start

```bash
# Install all tool dependencies
make kustomize controller-gen envtest golangci-lint chainsaw

# Generate CRDs and deepcopy methods
make manifests generate

# Run unit tests
make test

# Build the operator binary
make build

# Build and push the Docker image
make docker-build-and-push
```

## Development Workflow

### Code Generation

After modifying any `*_types.go` file under `api/v1alpha1/`:

```bash
make manifests generate
```

This regenerates:

- CRD manifests in `config/crd/bases/`
- RBAC role in `config/rbac/role.yaml`
- DeepCopy methods in `zz_generated.deepcopy.go`

### Running Locally

```bash
# Install CRDs into the current cluster
make install

# Run the operator against the current kubeconfig
make run
```

### Linting

```bash
make lint          # Check for issues
make lint-fix      # Auto-fix where possible
make lint-config   # Verify linter configuration
```

### Building Container Images

The default registry is `bedag/storagegrid-operator`. Most developers won't have push access to it. Override `IMAGE_REGISTRY` to use your own:

```bash
# Build and push to your own registry
make docker-build-and-push IMAGE_REGISTRY=my-registry.example.com/storagegrid-operator

# Build only (no push)
make docker-build IMAGE_REGISTRY=my-registry.example.com/storagegrid-operator
```

The image is automatically tagged with `:latest`, `:$GIT_COMMIT`, and `:$GIT_BRANCH` or `:$GIT_TAG`.

When deploying to a cluster, set `IMG` to match:

```bash
make deploy IMG=my-registry.example.com/storagegrid-operator:latest
```

## Testing

### Unit Tests

```bash
make test
```

Uses `envtest` for controller tests with a real API server but no real cluster.

### End-to-End Tests (Chainsaw)

The project uses [Kyverno Chainsaw](https://kyverno.github.io/chainsaw/) for declarative e2e tests. Tests are located under `test/e2e/chainsaw/`.

#### Initial Setup

1. **Configure git filters** (one-time, prevents committing real cluster values):

   ```bash
   make setup-git-filters
   ```

   This registers a git clean filter that automatically sanitizes `values.yaml` and `values-existing.yaml` when staging. Your local copies keep real values; committed versions contain `REPLACE_ME` placeholders. Requires `yq`.

2. **Configure test values** — values are prompted interactively the first time you run a test target. You can also edit directly:

   - `test/e2e/chainsaw/values.yaml` — for fresh infrastructure
   - `test/e2e/chainsaw/values-existing.yaml` — for pre-existing StorageGrid with namespace prefix

   Key fields:

   | Field                       | Description                                            |
   | --------------------------- | ------------------------------------------------------ |
   | `namespacePrefix`           | Prefix for test namespaces (empty for no prefix)       |
   | `storageGrid.name`          | Name of the StorageGrid CR in the cluster              |
   | `tenantClass.name`          | Name of the S3TenantClass CR                           |
   | `alternateTenantClass.name` | Second S3TenantClass for class-change tests (optional) |

#### Running Tests

```bash
# All chainsaw tests — fresh infrastructure
make test-chainsaw

# All chainsaw tests — existing StorageGrid
make test-chainsaw-existing

# S3Tenant tests only — fresh infrastructure
make e2e-s3tnt

# S3Tenant tests only — existing StorageGrid
make e2e-s3tnt-existing
```

If your values file still contains `REPLACE_ME` placeholders, the Make target will launch an interactive prompt to configure them before running tests.

Pass extra flags to Chainsaw via `CHAINSAW_ARGS`:

```bash
# Pause on failure for interactive debugging
make e2e-s3tnt-existing CHAINSAW_ARGS="--pause-on-failure"

# Or export for the whole session
export CHAINSAW_ARGS="--pause-on-failure"
make e2e-s3tnt-existing
```

#### Test Structure

```text
test/e2e/chainsaw/
  .chainsaw.yaml                    # Config for fresh infrastructure
  .chainsaw-existing.yaml           # Config with namespace prefix support
  values.yaml                       # Test values (git-sanitized)
  values-existing.yaml              # Test values for existing infra (git-sanitized)
  s3tenant/
    _step-templates/                # Reusable step templates
      verify-account-set-delete-policy.yaml
    lifecycle/                      # Full create → update → delete cycle
    deletion-protection/            # Annotation-based deletion protection
    ... other test scenarios
```

#### Writing Tests

Each test is a `chainsaw-test.yaml` in its own directory. Key conventions:

- **Top-level bindings**: Define `tenantName` (and any other test-scoped names) in `spec.bindings` and reference with `($tenantName)` in YAML resources or `$tenantName` in scripts.
- **Values references**: Use `($values.storageGrid.name)`, `($values.tenantClass.name)`, etc. for cluster-specific values.
- **Step templates**: Reuse shared logic via `use.template` referencing files in `_step-templates/`.
- **Cleanup**: If a test creates resources with deletion protection or other guards, add a `cleanup` block to remove the guard before Chainsaw's auto-cleanup runs.
- **Scripts vs native operations**: Prefer native Chainsaw operations (`assert`, `patch`, `delete` with `expect`) over scripts. Use scripts only when there's no native equivalent (e.g., capturing secret values, polling for deletion).

Example binding pattern:

```yaml
spec:
  bindings:
    - name: tenantName
      value: my-test
  steps:
    - name: create
      try:
        - create:
            resource:
              apiVersion: s3.bedag.ch/v1alpha1
              kind: S3Tenant
              metadata:
                name: ($tenantName)       # JMESPath — for YAML resources
    - name: check-secret
      try:
        - script:
            content: |
              kubectl get secret $tenantName-admin-credentials -n $NAMESPACE  # env var — for scripts
```

#### Chainsaw Configuration

Two configuration files support different environments:

| File                      | Use Case                                                                                    |
| ------------------------- | ------------------------------------------------------------------------------------------- |
| `.chainsaw.yaml`          | Fresh infrastructure, no namespace prefix                                                   |
| `.chainsaw-existing.yaml` | Existing cluster with namespace prefix (`join('-', [$values.namespacePrefix, $namespace])`) |

Timeouts: apply 30s, assert 2m, delete 2m, cleanup 2m. Tests use `failFast` mode.

## Git Filters for Values Sanitization

The chainsaw values files contain environment-specific names that should not be committed. A git clean filter handles this automatically:

```test
Working copy (real values) ──git add──▶ Clean filter (yq) ──▶ Staged with REPLACE_ME
```

**How it works:**

1. `.gitattributes` assigns the `chainsaw-values` filter to both values files
2. `make setup-git-filters` registers the filter in your local `.git/config`
3. On `git add`, `hack/sanitize-chainsaw-values.sh` replaces all values with `REPLACE_ME`
4. Your local files are never modified — only the staged version is sanitized

**Setup:** `make setup-git-filters` (required once per clone)

**Requires:** `yq` — if not installed, the filter exits with an error and the commit is blocked.

## Make Reference

Run `make help` for the full list. Key targets:

| Target                       | Description                                            |
| ---------------------------- | ------------------------------------------------------ |
| `make manifests generate`    | Regenerate CRDs, RBAC, and DeepCopy after type changes |
| `make test`                  | Unit tests with envtest                                |
| `make lint`                  | Lint with golangci-lint                                |
| `make build`                 | Build operator binary                                  |
| `make docker-build-and-push` | Build and push container image                         |
| `make install`               | Install CRDs into cluster                              |
| `make run`                   | Run operator locally against current kubeconfig        |
| `make deploy` / `undeploy`   | Deploy/remove operator in cluster                      |
| `make setup-git-filters`     | Configure git clean filter for values sanitization     |
| `make e2e-s3tnt-existing`    | Run S3Tenant e2e tests against existing StorageGrid    |
