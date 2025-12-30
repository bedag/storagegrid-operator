# Multi-Cluster Support - Future Architecture

**Status:** Deferred  
**Created:** 2024-12-24  
**Decision:** Implement single-cluster import first, defer multi-cluster until requested by users

---

## Executive Summary

Multi-cluster support would enable multiple Kubernetes clusters to manage or observe the same StorageGrid tenant. Imagine being able to have readonly tenants in multi-cluster with one being writing. This would allow you to create buckets for the same s3tenant on different clusters / namespaces. After analysis, we've decided to defer this feature in favor of a simpler, battle-tested import mechanism. This document captures our research and reasoning for future reference.

---

## Use Cases

### Use Case 1: Multi-Region Deployment
**Scenario:** Tenants accessed from multiple regions/clusters, managed centrally

**Requirements:**
- Single source of truth for configuration
- Regional clusters for low-latency access
- Consistent credential distribution
- Audit trail for changes

### Use Case 2: Dev/Staging/Prod Clusters
**Scenario:** Same tenant used across environment clusters

**Requirements:**
- Read-only access in non-prod
- Prevent accidental mutations
- Status visibility across environments
- Able to create buckets in each environment

### Use Case 3: S3tenant offered as a Service
**Scenario:** The tenant is managed by a central team, but multiple customers access it from their own clusters / namesoaces.

**Requirements:**
- Customers could "import" a readonly view of the tenant
- And possibly create buckets in their own clusters

---

## Architectural Options Explored

### Option A: Built-in Multi-Cluster

#### Design Sketch
```yaml
apiVersion: s3.bedag.ch/v1alpha1
kind: S3TenantAccount
metadata:
  name: shared-tenant
  annotations:
    multi-cluster.s3.bedag.ch/role: primary  # or "replica"
    multi-cluster.s3.bedag.ch/primary-cluster: "cluster-a"
spec:
  storageGridRef:
    name: my-grid
  # manually maintained list of clusters or other identifier
  multiCluster:
    enabled: true
    clusterName: cluster-a
    replicaClusters:
      - cluster-b
      - cluster-c
```

#### Implementation Requirements
- **Cluster Discovery:** How do clusters find each other?
  - Shared config (ConfigMap/Secret)
  - External registry service
  - DNS-based discovery
  
- **Leader Election:** Who's the primary?
  - etcd-based lease
  - First owner wins
  - Manual designation via annotation/label

- **State Synchronization:** How to replicate status?
  - Reconcile loop with polling interval
  - Push to message bus (NATS, Kafka)
  
- **Failure Scenarios:**
  - Network partition between clusters
  - Primary cluster down
  - Split-brain prevention
  - Credential rotation during failover?


#### Why We Rejected This
1. **Over-engineered** for current user needs
2. **High complexity** relative to benefit
3. **Distributed systems challenges** (consensus, failure modes)
4. **Maintenance burden** for rare use case
5. **Better alternatives exist** (see Option B)

---

### Option B: External Orchestration

#### Design Pattern
Use existing multi-cluster tools to deploy operator across clusters with different configs.

Didn't really followup on this idea, but here are some notes:

**Tools:**
- **GitOps (ArgoCD/Flux):** Deploy same tenant to multiple clusters
- **Crossplane:** Compose multi-cluster resources
- **KubeFed/Karmada:** Native Kubernetes federation
- **Cluster API:** Manage multiple clusters declaratively


#### Why This could be Better
1. **Leverage existing tools** - don't reinvent the wheel
2. **Clear ownership model** - explicit primary/replica designation
3. **Simpler failure handling** - GitOps reconciliation loop
4. **Operator stays simple** - focus on single-cluster correctness
5. **Users choose orchestration** - flexibility in tooling

---

## Current Decision: Simple Import Only

### What We're Implementing Now

**Single-Cluster Import with Ownership Enforcement**

```yaml
apiVersion: s3.bedag.ch/v1alpha1
kind: S3TenantAccount # only on account!
metadata:
  name: legacy-tenant
  annotations:
    admin.s3.bedag.ch/import-tenant-id: "12345-abcde"
spec:
  storageGridRef:
    name: my-grid
  # ... rest of spec
```

**Behavior:**
1. Operator fetches tenant `12345-abcde` from StorageGrid
2. Checks if tenant is already managed (reads metadata from description)
3. **If unmanaged:** Takes ownership, writes metadata, continues
4. **If managed by this CR:** Idempotent, continues
5. **If managed by different CR:** **Hard error, requires manual resolution**

**Metadata Written to Tenant Description:**
```
managed_by:storagegrid-operator
cluster_name:prod-k8s-cluster-01
cr_uid:abc-123-def-456
cr_name:legacy-tenant
kubernetes_namespace:default
managed_since:2024-12-24T10:00:00Z
```

### Why This Approach

**Simplicity:**
- ✅ Clear ownership model (one cluster owns, period)
   - -> Reconciliation fails if conflict detected
- ✅ Easy to understand and debug
- ✅ No distributed systems complexity
- ✅ Fast to implement and test

**Safety:**
- ✅ Hard error prevents accidental conflicts
- ✅ Manual resolution required (forces admin to think)
- ✅ No split-brain scenarios

**Future-Proof:**
- ✅ Metadata format extensible
- ✅ Can add multi-cluster later if needed
- ✅ External orchestration tools compatible
- ✅ Ussed to "Battle-test" single-cluster first

---

## Conflict Resolution Patterns

### Scenario: Tenant Imported in Two Clusters

**Setup:**
1. Cluster A imports tenant `abc-123`
2. Cluster B tries to import same tenant `abc-123`

**What Happens:**
```
Error: Cannot import tenant abc-123
Reason: Tenant is already managed by another resource
Details:
  - Managed By: storagegrid-operator
  - CR Name: legacy-tenant
  - CR UID: def-456-ghi-789
  - Managed Since: 2024-12-24T10:00:00Z

Resolution:
  To import this tenant in this cluster, the administrator must:
  1. Determine which cluster should manage this tenant
  2. Delete the S3TenantAccount from the other cluster (if it should migrate)
  3. Manually remove the metadata from the tenant description in StorageGrid
  4. Retry the import

  WARNING: Removing metadata from a managed tenant may cause the original
  cluster to recreate the tenant or enter an error state. Coordinate carefully.
```

**Manual Resolution Steps:**
```bash
# 1. Check which resource currently manages
kubectl get s3tenantaccount legacy-tenant -o yaml | grep uid

# 2. Decide which resource should own it
# Option A: Keep in Cluster A (abort Cluster B import)
# Option B: Migrate to Cluster B (continue below)

# 3. Delete from Cluster A
kubectl delete s3tenantaccount legacy-tenant 

# 4. Remove metadata from StorageGrid (via UI or API)
# Navigate to tenant -> Edit -> Description
# Remove lines starting with: managed_by, cr_uid, etc.

# 5. Retry import in Cluster B
kubectl apply -f tenant-cluster-b.yaml
```

---

## Future Implementation Considerations

### When to Revisit Multi-Cluster

**Triggers:**
- ✅ Multiple customers request multi-cluster support
- ✅ Import feature is stable and battle-tested (12+ months)
- ✅ Clear use case with requirements documented
- ✅ Resources available for 8-12 week project

**Pre-requisites:**
- ✅ Comprehensive integration tests for import
- ✅ Metrics/observability for import operations
- ✅ Well-documented operator behavior
- ✅ Stable API (v1beta1 or v1)

### Recommended Next Steps (When Revisiting)

1. **Survey users** - understand exact multi-cluster requirements
2. **Choose orchestration approach** - maybe external tool (ArgoCD/Crossplane)
3. **Consider new CRD** - `S3TenantAccountMirror` for read-only replicas
4. **Implement status-only mode** - mirror can sync status without mutations
5. **Add coordination protocol** - how mirrors discover primary
6. **Test failure scenarios** - network partitions, primary down, etc.

### API Evolution Path

**Phase 1 (Current):** Simple import
```yaml
kind: S3TenantAccount
metadata:
  annotations:
    admin.s3.bedag.ch/import-tenant-id: "abc-123"
```

**Phase 2 (Future):** Multi-cluster with external orchestration
```yaml
# Primary cluster
kind: S3TenantAccount
metadata:
  labels:
    multi-cluster.s3.bedag.ch/role: primary

---
# Replica clusters
kind: S3TenantAccountMirror
spec:
  primaryCluster: cluster-a
  tenantAccountRef:
    name: tenant-name
    cluster: cluster-a
```

**Phase 3 (Far Future):** Built-in coordination (if really needed)
```yaml
kind: S3TenantAccount
spec:
  multiCluster:
    enabled: true
    mode: primary
    coordinationEndpoint: https://cluster-coordinator.example.com
```

---

## Lessons Learned

### What Worked Well
- ✅ **Metadata in description field** - simple, no backend API changes needed
- ✅ **Annotation-based import** - declarative, kubectl-friendly
- ✅ **Hard errors on conflicts** - forces intentional resolution
- ✅ **Clear ownership model** - one cluster owns, period

### What Didn't Work
- ❌ **Read-only mode** - complex, many edge cases, limited value
- ❌ **Force-adopt annotation** - encourages dangerous operations
- ❌ **OwnsBackendResource field** - Adds more complexity with operator tracking ownership that might differ from actual backend state
- ❌ **Post-creation adoption** - confusing lifecycle, race conditions

### Design Principles Established
1. **Simplicity over flexibility** - solve 80% use case well
2. **Explicit over implicit** - require manual resolution of conflicts
3. **Battle-test first** - prove single-cluster before multi-cluster
4. **Leverage ecosystem** - use GitOps tools for orchestration
5. **Clear ownership** - one authoritative source, no ambiguity

---

## References

### Similar Operators
- **AWS Controllers for Kubernetes (ACK):** Uses adoption-policy annotation
- **GCP Config Connector:** Single cluster per resource
- **Azure Service Operator:** No built-in multi-cluster

### Reading
- [Kubernetes Multi-Cluster Patterns](https://kubernetes.io/docs/concepts/cluster-administration/federation/)

---

## Conclusion

We've chosen to implement simple, single-cluster import with hard ownership enforcement. This gives us:
- ✅ **Fast time-to-value** - users can import existing tenants now
- ✅ **Low complexity** - easy to understand, test, and maintain
- ✅ **Clear ownership** - no ambiguity about which cluster manages what
- ✅ **Future flexibility** - can add multi-cluster later with external tools

Multi-cluster support is deferred until:
1. Import is battle-tested and stable
2. Users explicitly request multi-cluster features
3. We have clear requirements and use cases
4. We have resources for proper implementation

**Next Steps:**
1. ✅ Implement simplified import (single-cluster only)
2. ✅ Remove read-only mode, force-adopt, ownership verification complexity
3. ✅ Add comprehensive tests for import feature
4. ✅ Document import process for users
5. ⏳ Monitor user feedback for multi-cluster requests
