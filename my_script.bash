#!/usr/bin/env bash
set -euo pipefail

NS="openshift-storage-client"
SA="ocs-client-operator-controller-manager"

echo "=== Before ==="
oc auth can-i list infrastructures.config.openshift.io \
  --as="system:serviceaccount:${NS}:${SA}" || true

ROLE="$(
  oc get clusterrolebinding -o json \
    | jq -r --arg ns "$NS" --arg sa "$SA" '
        .items[]
        | select(.subjects[]? | select(.kind=="ServiceAccount" and .namespace==$ns and .name==$sa))
        | select(.roleRef.kind=="ClusterRole")
        | .roleRef.name' \
    | head -n1
)"

if [[ -z "$ROLE" ]]; then
  echo "No ClusterRoleBinding found for ${NS}/${SA}" >&2
  exit 1
fi

echo "Patching ClusterRole: $ROLE"

oc patch clusterrole "$ROLE" --type=json -p='[
  {
    "op": "add",
    "path": "/rules/-",
    "value": {
      "apiGroups": ["config.openshift.io"],
      "resources": ["infrastructures"],
      "verbs": ["get", "list", "watch"]
    }
  }
]' || echo "Patch failed (rule may already exist)."

echo "=== After ==="
oc auth can-i list infrastructures.config.openshift.io \
  --as="system:serviceaccount:${NS}:${SA}"