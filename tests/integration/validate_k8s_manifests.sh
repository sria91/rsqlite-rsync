#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${BASH_VERSION:-}" ]]; then
  exec bash "$0" "$@"
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
validator_script="$repo_root/scripts/validate-k8s-manifests.sh"
original_path="$PATH"

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

pass_count=0

assert_contains() {
  local haystack="$1"
  local needle="$2"
  if [[ "$haystack" != *"$needle"* ]]; then
    echo "Expected output to contain: $needle" >&2
    echo "Actual output:" >&2
    echo "$haystack" >&2
    exit 1
  fi
}

assert_exit_code() {
  local actual="$1"
  local expected="$2"
  if [[ "$actual" -ne "$expected" ]]; then
    echo "Expected exit code $expected, got $actual" >&2
    exit 1
  fi
}

run_validator() {
  local manifests_csv="$1"
  local allowlist_csv="$2"
  local path_prefix="$3"
  local disable_kubeconform="$4"
  local disable_kubectl="$5"

  local output
  set +e
  output="$(
    PATH="$path_prefix:$original_path" \
    RSQLITE_RSYNC_MANIFEST_PATHS="$manifests_csv" \
    RSQLITE_RSYNC_ALLOW_CROSS_FILE_DUPLICATES="$allowlist_csv" \
    RSQLITE_RSYNC_DISABLE_KUBECONFORM="$disable_kubeconform" \
    RSQLITE_RSYNC_DISABLE_KUBECTL="$disable_kubectl" \
    "$validator_script" 2>&1
  )"
  local exit_code=$?
  set -e

  printf '%s\n' "$exit_code"
  printf '%s\n' "$output"
}

capture_result() {
  local manifests_csv="$1"
  local allowlist_csv="$2"
  local path_prefix="$3"
  local disable_kubeconform="${4:-0}"
  local disable_kubectl="${5:-0}"

  local result
  result="$(run_validator "$manifests_csv" "$allowlist_csv" "$path_prefix" "$disable_kubeconform" "$disable_kubectl")"

  result_code="${result%%$'\n'*}"
  if [[ "$result" == *$'\n'* ]]; then
    result_output="${result#*$'\n'}"
  else
    result_output=""
  fi
}

create_manifest() {
  local file="$1"
  local content="$2"
  cat >"$file" <<EOF
$content
EOF
}

next_case_dir() {
  local name="$1"
  local dir="$workdir/$name"
  mkdir -p "$dir"
  printf '%s\n' "$dir"
}

run_case() {
  local name="$1"
  shift
  echo "Running: $name"
  "$@"
  pass_count=$((pass_count + 1))
}

case_missing_manifest_fails() {
  local d
  d="$(next_case_dir missing_manifest)"

  local a="$d/a.yaml"
  local b="$d/missing.yaml"

  create_manifest "$a" "apiVersion: v1
kind: ConfigMap
metadata:
  name: one"

  capture_result "$a,$b" "" ""

  assert_exit_code "$result_code" 1
  assert_contains "$result_output" "Missing required manifest"
}

case_no_objects_fails() {
  local d
  d="$(next_case_dir no_objects)"

  local a="$d/a.yaml"
  local b="$d/b.yaml"

  create_manifest "$a" "apiVersion: v1"
  create_manifest "$b" "apiVersion: v1
kind: ConfigMap
metadata:
  name: two"

  capture_result "$a,$b" "" ""

  assert_exit_code "$result_code" 1
  assert_contains "$result_output" "No Kubernetes objects found in manifest"
}

case_duplicate_in_single_manifest_fails() {
  local d
  d="$(next_case_dir duplicate_single)"

  local a="$d/a.yaml"
  local b="$d/b.yaml"

  create_manifest "$a" "apiVersion: v1
kind: ConfigMap
metadata:
  name: same
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: same"

  create_manifest "$b" "apiVersion: v1
kind: Secret
metadata:
  name: other"

  capture_result "$a,$b" "" ""

  assert_exit_code "$result_code" 1
  assert_contains "$result_output" "Duplicate Kubernetes object identities found in a single manifest"
  assert_contains "$result_output" "ConfigMap/same"
}

case_unexpected_cross_file_duplicate_fails() {
  local d
  d="$(next_case_dir duplicate_cross_file)"

  local a="$d/a.yaml"
  local b="$d/b.yaml"

  create_manifest "$a" "apiVersion: v1
kind: ConfigMap
metadata:
  name: shared"

  create_manifest "$b" "apiVersion: v1
kind: ConfigMap
metadata:
  name: shared"

  capture_result "$a,$b" "" ""

  assert_exit_code "$result_code" 1
  assert_contains "$result_output" "Unexpected duplicate Kubernetes object identities across manifests"
  assert_contains "$result_output" "ConfigMap/shared"
}

case_allowlisted_cross_file_duplicate_passes() {
  local d
  d="$(next_case_dir allowlisted_duplicate)"

  local a="$d/a.yaml"
  local b="$d/b.yaml"

  create_manifest "$a" "apiVersion: v1
kind: ConfigMap
metadata:
  name: shared"

  create_manifest "$b" "apiVersion: v1
kind: ConfigMap
metadata:
  name: shared"

  capture_result "$a,$b" "ConfigMap/shared" ""

  assert_exit_code "$result_code" 0
}

case_kubeconform_backend_runs() {
  local d
  d="$(next_case_dir kubeconform_backend)"

  local a="$d/a.yaml"
  local b="$d/b.yaml"
  local fakebin="$d/fakebin"
  local marker="$d/kubeconform.args"

  mkdir -p "$fakebin"

  create_manifest "$a" "apiVersion: v1
kind: ConfigMap
metadata:
  name: one"

  create_manifest "$b" "apiVersion: v1
kind: Secret
metadata:
  name: two"

  cat >"$fakebin/kubeconform" <<EOF
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "\$*" > "$marker"
exit 0
EOF
  chmod +x "$fakebin/kubeconform"

  capture_result "$a,$b" "" "$fakebin" 0 0

  assert_exit_code "$result_code" 0
  assert_contains "$result_output" "Validating manifests with kubeconform"

  local args
  args="$(cat "$marker")"
  assert_contains "$args" "-strict"
  assert_contains "$args" "$a"
  assert_contains "$args" "$b"
}

case_kubectl_fallback_runs() {
  local d
  d="$(next_case_dir kubectl_backend)"

  local a="$d/a.yaml"
  local b="$d/b.yaml"
  local fakebin="$d/fakebin"
  local marker="$d/kubectl.log"

  mkdir -p "$fakebin"

  create_manifest "$a" "apiVersion: v1
kind: ConfigMap
metadata:
  name: one"

  create_manifest "$b" "apiVersion: v1
kind: Secret
metadata:
  name: two"

  cat >"$fakebin/kubectl" <<EOF
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "\$*" >> "$marker"
if [[ "\${1:-}" == "version" ]]; then
  exit 0
fi
if [[ "\${1:-}" == "apply" ]]; then
  exit 0
fi
exit 1
EOF
  chmod +x "$fakebin/kubectl"

  capture_result "$a,$b" "" "$fakebin" 1 0

  assert_exit_code "$result_code" 0
  assert_contains "$result_output" "kubeconform not found; using kubectl server-side dry-run fallback"

  local log
  log="$(cat "$marker")"
  assert_contains "$log" "version --request-timeout=2s"
  assert_contains "$log" "apply --dry-run=server -f $a"
  assert_contains "$log" "apply --dry-run=server -f $b"
}

case_no_backend_skips() {
  local d
  d="$(next_case_dir no_backend)"

  local a="$d/a.yaml"
  local b="$d/b.yaml"

  create_manifest "$a" "apiVersion: v1
kind: ConfigMap
metadata:
  name: one"

  create_manifest "$b" "apiVersion: v1
kind: Secret
metadata:
  name: two"

  capture_result "$a,$b" "" "" 1 1

  assert_exit_code "$result_code" 0
  assert_contains "$result_output" "Skipping Kubernetes manifest validation"
}

run_case "missing manifest fails" case_missing_manifest_fails
run_case "no objects fails" case_no_objects_fails
run_case "duplicate in single manifest fails" case_duplicate_in_single_manifest_fails
run_case "unexpected cross-file duplicate fails" case_unexpected_cross_file_duplicate_fails
run_case "allowlisted cross-file duplicate passes" case_allowlisted_cross_file_duplicate_passes
run_case "kubeconform backend runs" case_kubeconform_backend_runs
run_case "kubectl fallback runs" case_kubectl_fallback_runs
run_case "no backend skips" case_no_backend_skips

echo "All validator tests passed: $pass_count"
