#!/usr/bin/env bash
# Interactive guided setup for chainsaw values files.
# Prompts the user for each field that is still set to REPLACE_ME.
# Usage: hack/setup-chainsaw-values.sh <values-file>
set -euo pipefail

if ! command -v yq &>/dev/null; then
  echo "ERROR: yq is not installed — cannot configure values interactively." >&2
  echo "Install yq: https://github.com/mikefarah/yq#install" >&2
  echo "Alternatively, you can manually edit the values file and replace all REPLACE_ME placeholders." >&2
  exit 1
fi

VALUES_FILE="${1:?Usage: $0 <values-file>}"

if [ ! -f "$VALUES_FILE" ]; then
  echo "ERROR: Values file not found: $VALUES_FILE" >&2
  exit 1
fi

echo "=== Configuring $VALUES_FILE ==="
echo ""

changed=false

prompt_field() {
  local path="$1"
  local description="$2"
  local default="$3"
  local allow_empty="$4"

  current=$(yq eval "$path" "$VALUES_FILE")
  if [ "$current" != "REPLACE_ME" ]; then
    return
  fi

  if [ -n "$default" ]; then
    prompt="$description [default: $default]: "
  elif [ "$allow_empty" = "true" ]; then
    prompt="$description [press Enter for empty]: "
  else
    prompt="$description: "
  fi

  while true; do
    printf "%s" "$prompt"
    read -r value
    if [ -z "$value" ]; then
      if [ -n "$default" ]; then
        value="$default"
      elif [ "$allow_empty" = "true" ]; then
        value=""
      else
        echo "  This field is required. Please enter a value."
        continue
      fi
    fi
    break
  done

  yq eval -i "$path = \"$value\"" "$VALUES_FILE"
  changed=true
  echo "  -> Set $path = \"$value\""
  echo ""
}

prompt_field '.namespacePrefix'          'Namespace prefix (leave empty for no prefix)' '' 'true'
prompt_field '.storageGrid.name'         'StorageGrid CR name'                          '' 'false'
prompt_field '.tenantClass.name'         'S3TenantClass CR name'                        '' 'false'
prompt_field '.alternateTenantClass.name' 'Alternate S3TenantClass name (for class-change tests, optional)' '' 'true'

if [ "$changed" = true ]; then
  echo "=== $VALUES_FILE configured successfully ==="
else
  echo "=== All values already configured ==="
fi
