# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Kubebuilder/controller-runtime operator (`github.com/bedag/storagegrid-operator`, API group `s3.bedag.ch/v1alpha1`) that manages S3 resources on an **existing** NetApp StorageGrid installation. It does not manage the grid itself.

Backend access happens over two distinct channels:

- **StorageGrid management API** via `pkg/grid/` (uses `github.com/bedag/storagegrid-sdk-go`) — tenants, users, groups, buckets, policies, grid health.
- **S3 API** via `pkg/s3/` (AWS SDK v2) — bucket policies, tagging, lifecycle, drain. Requires network access to the S3 loadbalancer endpoint, which the management API path does not.

## Commands

```bash
make test           # unit + envtest tests (runs manifests, generate, fmt, vet first)
make lint           # golangci-lint; lint-fix / lint-config also exist
make build          # binary to bin/manager
make manifests generate   # REQUIRED after any change under api/v1alpha1/
make run            # run against current kubeconfig (make install first for CRDs)
```

Tool binaries (controller-gen, kustomize, golangci-lint, chainsaw, setup-envtest, envtest k8s assets) are auto-downloaded into `bin/` by the Make targets.

### Running a single test

Controller and webhook tests are Ginkgo suites on envtest, so they need `KUBEBUILDER_ASSETS`:

```bash
make setup-envtest   # once, downloads the matching k8s assets into bin/k8s
KUBEBUILDER_ASSETS="$(bin/setup-envtest use 1.34 --bin-dir bin -p path)" go test ./internal/controller/... -args -ginkgo.focus="S3Tenant"
```

The `1.34` tracks the `k8s.io/api` minor version in `go.mod` (the Makefile derives it); bump it when that dependency moves. Plain Go tests (e.g. `pkg/`) run with a bare `go test ./pkg/grid/...`.

### E2E (Chainsaw)

E2E tests run against a **real StorageGrid** — there is no fake backend. Config lives in `test/e2e/chainsaw/`; `values.yaml` / `values-existing.yaml` are sanitized to `REPLACE_ME` on `git add` by a clean filter that must be installed once with `make setup-git-filters` (needs `yq`). Targets: `make test-chainsaw`, `make test-chainsaw-existing`, `make e2e-s3tnt`, `make e2e-s3tnt-existing`; extra flags via `CHAINSAW_ARGS="--pause-on-failure"`. See [CONTRIBUTING.md](CONTRIBUTING.md) for test-authoring conventions (bindings, step templates, prefer native operations over scripts).

`make test-e2e` is the separate Kind-based Go e2e suite and is largely vestigial.

## Resource model

```
StorageGrid (cluster)          connection + grid-wide defaults
  └── S3TenantClass (cluster)  a loadbalancer endpoint, IngressClass-like
  └── S3TenantAccount (cluster)  the real StorageGrid tenant  ← "PersistentVolume"
        └── S3Tenant (ns)        namespace-local claim on an account  ← "PersistentVolumeClaim"
              └── S3Bucket (ns)
                    └── S3Access (ns)  dedicated S3 user + credentials, scoped by policies
GlobalS3Policy (cluster) / S3Policy (ns)  reusable rule sets referenced by S3Access
```

The PV/PVC split is the central design decision: **all backend interaction happens in the S3TenantAccount controller**, never in S3Tenant. S3Tenant only creates/claims/binds an account and mirrors its status. Accounts move through `PhaseReady` (unbound, claimable) → `PhaseBound` → `PhaseRetainThenDelete`/`PhaseDeleting`, with a deletion policy (`Delete` / `Retain` / `RetainThenDelete`) deciding what happens to the backend tenant.

Ownership of pre-existing backend resources is tracked differently per kind: **tenants** via metadata embedded in the StorageGrid tenant *description* (`managed_by`, `cr_uid`, …, since NetApp has no tenant tags); **buckets** via four `s3.bedag.ch/*` S3 bucket tags. Both import paths refuse to take over a resource owned by another CR unless forced.

## Layering rules

- `internal/controller/` — Kubernetes only: fetching resources, orchestration, conditions, finalizers, events, cross-resource references.
- `internal/webhook/` — admission validation/defaulting.
- `pkg/grid/`, `pkg/s3/` — all backend calls, data transformation, backend-side business rules. No Kubernetes types, no conditions, no finalizers.
- `pkg/kube/` — reusable Kubernetes helpers (secret create/fetch/finalizer, owned-secret reconciliation, connection-details projection, quantity parsing).

Putting SDK model structs or backend branching inside a controller is the main thing reviewers push back on. See [docs/architecture/separation-of-concerns.md](docs/architecture/separation-of-concerns.md).

## Controller conventions

Every controller follows the same shape (see [docs/architecture/controller-patterns.md](docs/architecture/controller-patterns.md)):

1. `Reconcile` holds only boilerplate — fetch the object, build a per-loop `*xReconcileContext` (`rctx`) holding the primary resource, related resources, backend clients, cached backend data, and the `ObjectUpdated` / `DoRequeue` flags.
2. `doReconcile(ctx, rctx)` orchestrates small, idempotent, single-purpose `reconcile<Component>` steps; each sets conditions on failure rather than returning early with a bare error.
3. Back in `Reconcile`: `deriveReadiness` maps conditions to readiness, then — **in this order** — if `rctx.ObjectUpdated`, `r.Update()` the object (annotations/finalizers), restoring `Status` from a deep copy afterwards because the update round-trip discards it; then always `r.Status().Patch(ctx, obj, client.MergeFrom(statusBase))` where `statusBase` was deep-copied before `doReconcile`.

Getting the update/status ordering wrong silently drops either annotations or status — match the existing code exactly.

Shared vocabularies live in one place each and should be extended there, not inlined:

- Condition types: [api/v1alpha1/condition_types.go](api/v1alpha1/condition_types.go)
- Annotations (all derived from `Domain` + a prefix): [api/v1alpha1/annotations.go](api/v1alpha1/annotations.go)
- Event reasons: [internal/controller/events.go](internal/controller/events.go). Events are emitted immediately but only on state *change*, and never propagated across resources — each CR has its own event stream ([docs/architecture/events.md](docs/architecture/events.md)).

Field indexes (`status.s3EndpointConfig.s3TenantClassName`, `.spec.policyRefs`) are registered in [cmd/main.go](cmd/main.go); a new watch that filters on a spec/status field needs its index added there too.

## Linting gotchas

`.golangci.yml` enables a strict set including `godot` (**every comment must end with a period** — hence the trailing dots throughout) and `goheader` against `hack/boilerplate.go.txt` (**every new `.go` file needs the Apache license header**). `dupl` is on with threshold 200; structurally similar reconcilers carry `//nolint:dupl` with a justification.

## Runtime configuration

- `OPERATOR_NAMESPACE` — where tenant root credential secrets are stored (defaults to `storagegrid-operator-system`).
- `ENABLE_WEBHOOKS=false` — skips webhook registration, useful for `make run` without certs.

## Contributing

PR titles must follow Conventional Commits (`feat(scope): …`, `fix(api): …`). User-facing behavior changes belong in [.github/README.md](.github/README.md), which is the project's public README and documents every CRD field, annotation, and workflow in detail.
