# Grafana dashboards

Importable dashboards for the storagegrid-operator metrics. See the "Monitoring" section of the [README](../.github/README.md) for the metrics themselves.

| File | Title | For |
| --- | --- | --- |
| `storagegrid-operator-overview.json` | StorageGrid Operator / Overview | Operator admins: grid and endpoint health, tenant usage and quota across clusters, resource state, StorageGrid API calls |
| `storagegrid-s3-usage.json` | S3 Storage / Usage | Namespace users: usage and quota of their S3Tenants, bucket usage |
| `storagegrid-s3-resources.json` | S3 Storage / Resources | Namespace users: phase and readiness of their S3Tenants, S3Buckets and S3Accesses |

## Import

1. In Grafana, go to **Dashboards → New → Import** and upload the JSON file.
2. Open the dashboard and pick your Prometheus datasource in the **Data source** variable.

## Notes

- Every query filters on a `cluster` label. If your series have no `cluster` label (single cluster, no external labels), the **Cluster** variable stays empty and the filter `cluster=~".*"` still matches.
- The user dashboards show whatever namespaces the selected datasource holds. With a datasource scoped to a team's namespaces, the team sees only its own tenants and buckets.
- Bucket panels stay empty unless `spec.metrics.bucketUsage` is enabled on the StorageGrid.
- The overview needs `/metrics/grid` to be scraped (see `config/prometheus/monitor.yaml`) for usage, endpoint and grid scrape panels.
