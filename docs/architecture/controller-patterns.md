# Controller Architecture Patterns

This document explains the core architectural patterns used throughout the StorageGrid Operator controllers, focusing on the `doReconcile` method pattern and reconciliation context (`rctx`) structure.

## Overview

The StorageGrid Operator implements a consistent controller architecture across all five controllers (`StorageGrid`, `S3TenantClass`, `S3TenantAccount`, `S3Tenant`, and `S3Bucket`). This architecture emphasizes modularity, maintainability, and operational excellence through standardized patterns.

## Core Patterns

### 1. The `doReconcile` Pattern

#### What is it?

The `doReconcile` pattern is a structured approach to Kubernetes controller reconciliation that separates the main reconciliation logic from the Kubernetes controller-runtime boilerplate.

```go
func (r *S3TenantAccountReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    // Standard boilerplate: logging, resource fetching, etc.
    log := log.FromContext(ctx).WithValues("s3tenantaccount", req.NamespacedName)
    
    rctx := &accountReconcileContext{
        Account: &s3v1alpha1.S3TenantAccount{},
        // ... other context fields
    }
    
    if err := r.Get(ctx, req.NamespacedName, rctx.Account); err != nil {
        return ctrl.Result{}, client.IgnoreNotFound(err)
    }
    
    // Delegate to doReconcile for the actual logic
    err := r.doReconcile(ctx, rctx)
    
    // Handle status updates and error handling
    return ctrl.Result{Requeue: rctx.DoRequeue}, err
}

func (r *S3TenantAccountReconciler) doReconcile(ctx context.Context, rctx *accountReconcileContext) error {
    // All reconciliation logic happens here
    if err := r.reconcileStorageGrid(ctx, rctx); err != nil {
        return err
    }
    
    if err := r.reconcileTenantClass(ctx, rctx); err != nil {
        return err
    }
    
    // ... more reconciliation steps
    return nil
}
```

#### Why this pattern?

1. **Separation of Concerns**: Separates Kubernetes controller mechanics from business logic
2. **Testability**: `doReconcile` can be unit tested without controller-runtime dependencies
3. **Consistency**: All controllers follow the same pattern, reducing cognitive load
4. **Error Handling**: Centralized error handling, status management and object updates
5. **Modularity**: Each reconciliation step is a separate, focused function

#### Benefits

- ✅ **Reduced Boilerplate**: Common patterns are handled once in the main `Reconcile` method
- ✅ **Better Testing**: Business logic can be tested independently
- ✅ **Easier Debugging**: Clear separation between framework and application logic
- ✅ **Maintainability**: Consistent structure across all controllers

### 2. Reconciliation Context (`rctx`) Pattern

#### What is it?

The reconciliation context is a struct that holds all the data needed during a single reconciliation loop. It acts as a shared state container passed between reconciliation functions.

```go
type accountReconcileContext struct {
    Account       *s3v1alpha1.S3TenantAccount
    SG            *s3v1alpha1.StorageGrid
    TenantClass   *s3v1alpha1.S3TenantClass
    DoRequeue     bool
    ObjectUpdated bool
    
    // Backend clients
    GridClient   *grid.GridClient
    TenantClient *grid.TenantClient
    
    // Backend data
    BackendTenant      *models.Tenant
    BackendTenantUsage *models.TenantUsage
}
```

#### Why this pattern?

1. **State Management**: Keeps related data together for the duration of reconciliation
2. **Performance**: Avoids repeated API calls by caching fetched resources
3. **Thread Safety**: Each reconciliation gets its own context instance
4. **Data Consistency**: Ensures all reconciliation steps work with the same data snapshot
5. **Explicit Dependencies**: Makes resource relationships visible and explicit

#### Context Fields

Each controller's context typically includes:

- **Primary Resource**: The main resource being reconciled
- **Dependent Resources**: Related Kubernetes resources needed for reconciliation
- **Control Flags**: `DoRequeue`, `ObjectUpdated` for flow control
- **Backend Clients**: Initialized clients for external API calls
- **Backend Data**: Cached data from external systems (StorageGrid API)

#### Benefits

- ✅ **Performance**: Single fetch per reconciliation loop
- ✅ **Consistency**: All functions work with the same data snapshot
- ✅ **Clarity**: Explicit about what data each function needs
- ✅ **Memory Efficiency**: Context is garbage collected after reconciliation

## Modular Reconciliation Functions

### Function Naming Convention

All reconciliation functions follow a consistent naming pattern:

```go
func (r *Controller) reconcile<Component>(ctx context.Context, rctx *reconcileContext) error
```

Examples:
- `reconcileStorageGrid()` - Fetch and validate StorageGrid reference
- `reconcileTenantClass()` - Ensure tenant class exists and is ready
- `reconcileBackendTenant()` - Create or update tenant in StorageGrid
- `reconcileCredentials()` - Manage authentication credentials
- `reconcileUsageTracking()` - Update usage statistics

### Single Responsibility Principle

Each reconciliation function has a single, well-defined responsibility:

```go
func (r *S3TenantAccountReconciler) reconcileStorageGrid(ctx context.Context, rctx *accountReconcileContext) error {
    // Only responsible for:
    // 1. Fetching the StorageGrid resource
    // 2. Validating it's ready
    // 3. Storing it in rctx.SG
    
    if err := r.Get(ctx, storageGridKey, rctx.SG); err != nil {
        return fmt.Errorf("failed to get StorageGrid: %w", err)
    }
    
    if rctx.SG.Status.Phase != s3v1alpha1.PhaseReady {
        return fmt.Errorf("StorageGrid %s is not ready", rctx.SG.Name)
    }
    
    return nil
}
```

### Idempotent Operations

Every reconciliation function is designed to be idempotent:

```go
func (r *S3TenantAccountReconciler) reconcileBackendTenant(ctx context.Context, rctx *accountReconcileContext) error {
    // Check if tenant already exists
    if rctx.Account.Status.TenantID != "" {
        log.V(1).Info("Tenant already exists, fetching current state")
        return r.fetchExistingTenant(ctx, rctx)
    }
    
    // Only create if it doesn't exist
    log.V(1).Info("Creating new tenant")
    return r.createNewTenant(ctx, rctx)
}
```

## Condition Management

### Centralized Condition Handling

Conditions are primarily managed within the `doReconcile` method or dedicated condition reconciliation functions:

```go
func (r *S3TenantAccountReconciler) doReconcile(ctx context.Context, rctx *accountReconcileContext) error {
    // Each step sets appropriate conditions on failure
    if err := r.reconcileStorageGrid(ctx, rctx); err != nil {
        r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeFailed, metav1.ConditionTrue, 
            "StorageGridNotReady", err.Error())
        return err
    }
    
    if err := r.reconcileBackendTenant(ctx, rctx); err != nil {
        r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeFailed, metav1.ConditionTrue, 
            "TenantCreationFailed", err.Error())
        return err
    }
    
    // Success - set ready condition
    r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReady, metav1.ConditionTrue, 
        "TenantReady", "Tenant is ready for use")
    
    return nil
}
```

### Condition Standardization

Most controllers use the same condition types and follow consistent patterns:

- `Ready`: True or false whether the resource is fully operational
- `ReconcileSucceeded`: True or false based on last reconciliation
- `BackingResourceReady`: Whether dependent resources are ready
- `Created`: On `S3TenantAccount`, `S3Tenant` and `S3Bucket` indicates backend creation
- `Pending`: Long-running operation in progress

## Error Handling Strategy

### Structured Error Handling

Errors are wrapped with context and appropriate conditions are set:

```go
func (r *S3TenantAccountReconciler) reconcileBackendTenant(ctx context.Context, rctx *accountReconcileContext) error {
    tenant, err := grid.CreateTenant(ctx, tenantRequest)
    if err != nil {
        // Wrap error with context
        wrappedErr := fmt.Errorf("failed to create tenant %s: %w", rctx.Account.Name, err)
        
        // Set appropriate condition
        r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeFailed, metav1.ConditionTrue, 
            "TenantCreationFailed", wrappedErr.Error())
        
        return wrappedErr
    }
    
    // Success path...
    return nil
}
```

### Graceful Degradation

Non-critical operations don't fail the entire reconciliation:

```go
func (r *S3TenantAccountReconciler) doReconcile(ctx context.Context, rctx *accountReconcileContext) error {
    // Critical operations - fail on error
    if err := r.reconcileBackendTenant(ctx, rctx); err != nil {
        return err
    }
    
    // Non-critical operations - log but continue
    if err := r.reconcileUsageTracking(ctx, rctx); err != nil {
        log.Error(err, "Failed to update usage tracking, continuing")
        r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeDegraded, metav1.ConditionTrue,
            "UsageTrackingFailed", "Usage tracking unavailable")
    }
    
    return nil
}
```

## Benefits of This Architecture

### 1. **Maintainability**
- Consistent patterns across all controllers
- Clear separation of concerns
- Easy to understand and modify

### 2. **Testability**
- Each reconciliation function can be unit tested
- Mock contexts can be created for testing
- Business logic is separate from controller framework

### 3. **Debugging**
- Clear logging at each step
- Structured error messages
- Explicit state management

### 4. **Performance**
- Efficient resource caching
- Minimal API calls per reconciliation
- Optimized condition management

### 5. **Reliability**
- Idempotent operations
- Graceful error handling
- Comprehensive status reporting

## Implementation Examples

### Simple Controller (S3TenantClass)

```go
func (r *S3TenantClassReconciler) doReconcile(ctx context.Context, rctx *tenantClassReconcileContext) error {
    // Step 1: Validate StorageGrid
    if err := r.reconcileStorageGrid(ctx, rctx); err != nil {
        r.setCondition(rctx.TenantClass, s3v1alpha1.ConditionTypeFailed, metav1.ConditionTrue, 
            "StorageGridNotReady", err.Error())
        return err
    }
    
    // Step 2: Validate endpoint
    if err := r.reconcileEndpointValidation(ctx, rctx); err != nil {
        r.setCondition(rctx.TenantClass, s3v1alpha1.ConditionTypeFailed, metav1.ConditionTrue, 
            "EndpointValidationFailed", err.Error())
        return err
    }
    
    // Success
    r.setCondition(rctx.TenantClass, s3v1alpha1.ConditionTypeReady, metav1.ConditionTrue, 
        "TenantClassReady", "TenantClass is ready for use")
    
    return nil
}
```

### Complex Controller (S3TenantAccount)

```go
func (r *S3TenantAccountReconciler) doReconcile(ctx context.Context, rctx *accountReconcileContext) error {
    // Phase 1: Validate dependencies
    if err := r.reconcileStorageGrid(ctx, rctx); err != nil {
        return r.handleDependencyError(ctx, rctx, "StorageGrid", err)
    }
    
    if err := r.reconcileTenantClass(ctx, rctx); err != nil {
        return r.handleDependencyError(ctx, rctx, "TenantClass", err)
    }
    
    // Phase 2: Manage backend tenant
    if err := r.reconcileBackendTenant(ctx, rctx); err != nil {
        return r.handleBackendError(ctx, rctx, "Tenant", err)
    }
    
    // Phase 3: Manage credentials and access
    if err := r.reconcileCredentials(ctx, rctx); err != nil {
        return r.handleCredentialError(ctx, rctx, err)
    }
    
    // Phase 4: Non-critical operations
    r.reconcileUsageTracking(ctx, rctx) // Best effort
    r.reconcileMetadataSync(ctx, rctx)   // Best effort
    
    // Success
    return r.setReadyCondition(ctx, rctx)
}
```

## Best Practices

### 1. **Function Design**
- Keep functions focused on a single responsibility
- Make operations idempotent
- Use descriptive function names
- Return meaningful errors

### 2. **Context Usage**
- Initialize context at the start of reconciliation
- Pass context by pointer to avoid copying
- Don't store contexts beyond reconciliation scope
- Use context for caching, not for flow control

### 3. **Error Handling**
- Always wrap errors with context
- Set appropriate conditions on errors
- Distinguish between retriable and permanent errors
- Use structured logging for debugging

### 4. **Condition Management**
- Set conditions consistently across controllers
- Remove conflicting conditions when setting success states
- Use meaningful reason codes and messages
- Update conditions promptly during reconciliation

This architecture has proven effective for managing the complexity of the StorageGrid Operator while maintaining high code quality, reliability, and maintainability across all five controllers.