#!/usr/bin/env bash
# Git clean filter: sanitizes chainsaw values files before staging.
# Replaces environment-specific values with REPLACE_ME placeholders.
# Used via: git config filter.chainsaw-values.clean hack/sanitize-chainsaw-values.sh
set -euo pipefail

if ! command -v yq &>/dev/null; then
  echo "ERROR: yq is not installed — manual sanitization required." >&2
  echo "Install yq: https://github.com/mikefarah/yq#install" >&2
  exit 1
fi

# Read stdin, replace all known leaf values with REPLACE_ME placeholder.
yq eval '
  .namespacePrefix = "REPLACE_ME" |
  .storageGrid.name = "REPLACE_ME" |
  .tenantClass.name = "REPLACE_ME" |
  .alternateTenantClass.name = "REPLACE_ME"
'
