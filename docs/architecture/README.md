# StorageGrid Operator Architecture Documentation

This directory contains comprehensive architecture documentation for the StorageGrid Operator, explaining the design decisions, patterns, and internal structure of the project.

## 📚 Documentation Index

### Core Architecture

#### [Controller Patterns](./controller-patterns.md)
Explains the fundamental architectural patterns used across all controllers in the StorageGrid Operator:

- **doReconcile Pattern**: Separation of Kubernetes controller mechanics from business logic
- **Reconciliation Context (rctx)**: Structured state management during reconciliation
- **Modular Functions**: Single-responsibility reconciliation steps
- **Condition Management**: Standardized status reporting across all resources
- **Error Handling**: Structured error management and recovery strategies

*Essential reading for developers working on controller logic.*

#### [Separation of Concerns](./separation-of-concerns.md)
Details the clear architectural boundaries between different layers of the system:

- **Controller Layer** (`internal/controller/`): Kubernetes-native operations and orchestration
- **Grid Package** (`pkg/grid/`): StorageGrid backend interactions and business logic  
- **Utility Package** (`pkg/kube/`): Reusable Kubernetes operations
- **Testing Strategy**: How to test each layer independently
- **Guidelines**: Where to add new functionality

*Critical for understanding where to place new code and maintaining clean architecture.*

#### [Tenant Relationship](./tenant-relationship.md)
Explains the relationship between `S3Tenant` and `S3TenantAccount` resources:

- **S3Tenant**: Namespace-scoped resource representing a tenant within a specific Kubernetes namespace. Provides the user-facing interface for managing tenant-related configurations and policies.
- **S3TenantAccount**: Cluster-scoped resource managing the actual StorageGrid tenant. Handles backend operations such as creation, updates, and deletion of the tenant in StorageGrid.

*Necessary for understanding how tenants are modeled and managed.*

## 🏗️ Architectural Principles

The StorageGrid Operator is built on several key architectural principles:

### 1. **Clean Separation of Concerns**
- Clear boundaries between Kubernetes operations and StorageGrid backend logic
- Each package has well-defined responsibilities
- Minimal cross-layer dependencies

### 2. **Consistent Controller Patterns**
- All five controllers follow the same `doReconcile` pattern
- Standardized reconciliation context structure
- Uniform error handling and condition management

### 3. **Modularity and Reusability**
- Individual reconciliation functions with single responsibilities
- Reusable utility functions across controllers
- Grid package functions shared between controllers

### 4. **Operational Excellence**
- Comprehensive status reporting with conditions
- Structured logging throughout the system
- Idempotent operations for reliability

### 5. **Developer Experience**
- Clear guidelines for adding new functionality
- Consistent patterns reduce cognitive load
- Comprehensive documentation and examples

## 🎯 Quick Reference

### For New Developers

1. **Start with**: [Controller Patterns](./controller-patterns.md) to understand the basic structure
2. **Then read**: [Separation of Concerns](./separation-of-concerns.md) to understand where to add code
3. **Follow**: The guidelines in each document for adding new functionality

### Understanding the Flow

1. **User creates** an `S3Tenant` resource
2. **S3Tenant controller** creates an `S3TenantAccount` 
3. **S3TenantAccount controller** interacts with StorageGrid backend
4. **Status propagates** back through the resource hierarchy
5. **User creates** `S3Bucket` resources within the tenant

---

This architecture documentation is designed to help developers understand and contribute to the StorageGrid Operator effectively. Each document builds upon the others to provide a comprehensive view of the system's design and implementation.