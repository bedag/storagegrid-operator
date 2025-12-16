# Event Architecture

This document describes the event recording implementation in the StorageGrid Operator and the design decisions behind it.

## Overview

The operator emits Kubernetes events to provide observability into controller operations and state changes. Events are recorded for significant operations, errors, and state transitions across all five controllers.

**Event constant definitions:** [`internal/controller/events.go`](../../internal/controller/events.go)

## Design Decisions

### Pattern Selection: PV/PVC Approach

We chose to implement events similar to how Kubernetes handles PersistentVolumes and PersistentVolumeClaims - each controller emits its own events without propagating them across related resources.

**What this means:**
- `S3TenantAccount` emits events about backend tenant operations
- `S3Tenant` emits events about account binding and configuration
- Events are visible on the specific resource where the operation occurred

**Alternatives considered:**

1. **Cross-resource propagation**: Bubble events from S3TenantAccount up to S3Tenant
   - Rejected: Adds complexity and event noise
   - Rejected: Obscures which component is responsible for what

2. **Single event stream**: All events on one resource
   - Rejected: Violates separation of concerns
   - Rejected: Makes troubleshooting harder

3. **No events**: Rely solely on logs and conditions
   - Rejected: Events provide user-visible timeline without log access
   - Rejected: Standard Kubernetes pattern expected by operators

### Emission Timing: Immediate Emission

Events are emitted immediately when operations occur using the `emitEvent()` method.

**Implementation:**
```go
// Emit event immediately when operation occurs
r.emitEvent(rctx, corev1.EventTypeWarning, EventTenantCreateFailed, 
    fmt.Sprintf("Failed to create tenant: %v", err))
```

**Why immediate instead of batching:**
- Simpler implementation - no queue management needed
- Events appear in real-time as operations occur
- Same number of API calls either way (no batching API in Kubernetes)
- Events naturally reflect the operation timeline

**Alternatives considered:**

1. **End-of-reconciliation batching**: Queue and emit all at once
   - Rejected: No actual batching in Kubernetes event API (same number of calls)
   - Rejected: Added complexity with queue management for no benefit
   - Rejected: Delayed visibility of events until reconciliation completes

2. **Conditional emission with queuing**: Complex state tracking
   - Rejected: Over-engineered for the problem
   - Rejected: Kubernetes already deduplicates similar events 

### Event Frequency: State-Change Only

Events are emitted only when state actually changes, not on every reconciliation.

**Examples:**
- ✅ Emit when tenant transitions from creating → ready
- ✅ Emit when grid transitions from unhealthy → healthy
- ✅ Emit when credentials are rotated (user action)
- ❌ Don't emit "BucketReady" on every successful reconciliation
- ❌ Don't emit "GridHealthy" when grid remains healthy

**Why state-change only:**
- Prevents event spam in steady-state
- Follows standard Kubernetes controller patterns
- Events remain useful signals, not noise

**Examples not available:**
- `EventBucketReady` - fired every reconciliation when bucket was ready
- `EventGridHealthy` - fired every health check when grid remained healthy (kept `EventGridHealthRecovered` for transitions)
- `EventAllowlistSyncCompleted` - fired every sync regardless of changes

**Periodic events (kept):**
- `EventGatewayStatusRefreshed` - fires only when `spec.refreshInterval` elapses, not every reconciliation

## Implementation Details

### Controller Pattern

Each controller follows the same pattern:

```go
type XReconciler struct {
    client.Client
    Scheme   *runtime.Scheme
    Recorder record.EventRecorder  // Injected in main.go
}

type xReconcileContext struct {
    Resource *v1alpha1.X
}

func (r *XReconciler) emitEvent(rctx *xReconcileContext, eventType, reason, message string) {
    r.Recorder.Event(rctx.Resource, eventType, reason, message)
}
```

### Event Recorder Setup

Event recorders are created and injected in `cmd/main.go`:

```go
eventRecorder := mgr.GetEventRecorderFor("s3bucket-controller")

if err = (&controller.S3BucketReconciler{
    Client:   mgr.GetClient(),
    Scheme:   mgr.GetScheme(),
    Recorder: eventRecorder,
}).SetupWithManager(mgr); err != nil {
    // handle error
}
```

Each controller has its own recorder with a distinct name for event source identification.

### RBAC Requirements

Controllers require permissions to create and patch events:

```yaml
# +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
```

Generated RBAC manifests include these permissions automatically.

## Event Categories

Events are organized by controller and operation type. See [`internal/controller/events.go`](../../internal/controller/events.go) for the complete list (64 unique event reasons).

### Critical Events

**S3 Endpoint Access** (S3Bucket controller):
- `EventS3EndpointConnectionFailed`: Cannot reach S3 loadbalancer
- `EventS3EndpointConnectionEstablished`: S3 endpoint accessible

These events are critical for diagnosing network connectivity issues between the operator and StorageGrid's S3 loadbalancer endpoints. If bucket policy operations are failing, check for these events.

**Backend Connection** (S3TenantAccount controller):
- `EventBackendConnectionFailed`: Cannot reach StorageGrid management API
- `EventBackendConnectionRestored`: Connection to management API restored

**Health Monitoring** (StorageGrid controller):
- `EventGridUnhealthy`: Grid has too many unavailable nodes
- `EventGridHealthRecovered`: Grid has recovered from unhealthy state

### State Transitions

Events marking significant state changes:
- Lifecycle: Creating → Created → Deleting → Deleted
- Health: Unhealthy → Recovered
- Quota: Normal → Warning → Exceeded → Normal

### Error Conditions

Warning/error events for failed operations:
- `*Failed` events indicate operation failures
- Include error details in message for debugging
- Always paired with condition updates

## Usage Patterns

### Adding New Events

1. **Define constant** in `internal/controller/events.go`:
   ```go
   const EventNewOperation = "NewOperation"
   ```

2. **Emit during reconciliation**:
   ```go
   r.emitEvent(rctx, corev1.EventTypeNormal, EventNewOperation,
       fmt.Sprintf("Operation completed: %s", details))
   ```

3. **Only emit on state changes**:
   ```go
   if previousState != currentState {
       r.emitEvent(rctx, ...)
   }
   ```

### Viewing Events

Events are visible on the resource where they occurred. Use standard kubectl commands or Kubernetes dashboard to view them.

## Testing

Event emission can be verified in controller tests by inspecting the recorded events on the fake event recorder. See existing controller tests for examples.
