#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Elasticsearch ILM Report + Policies Export - FIXED BACKING_DATA_STREAM
# - Sizes in raw bytes
# - Only indices in report
# - BACKING_DATA_STREAM inferred from index name pattern (no _data_stream call)
# =============================================================================

echo "Script started at $(date)"

read -p "Elasticsearch username: " ES_USER
read -s -p "Elasticsearch password: " ES_PASS
echo -e "\n"

ES_PROTO="http"                    # change to "https" if needed
ES_HOST="localhost:9200"
ES_URL="${ES_PROTO}://${ES_HOST}"

echo "=== Connecting to: ${ES_URL} ==="

# ───────────────────────────────────────────────────────────────
echo -e "\nFetching indices with sizes (raw bytes) and ILM info..."

indices_file="indices.json"
curl -s -u "${ES_USER}:${ES_PASS}" \
  "${ES_URL}/_cat/indices?format=json&h=index,store.size,pri.store.size,ilm.policy&bytes=b" > "${indices_file}"

if [ ! -s "${indices_file}" ]; then
  echo "ERROR: Failed to fetch indices"
  exit 1
fi

echo "indices.json size: $(wc -c < "${indices_file}") bytes"

# ───────────────────────────────────────────────────────────────
echo -e "\nBuilding report (only indices)..."

report_file="report.tsv"

# Header
echo -e "INDEX\tSTORE_BYTES\tPRI_STORE_BYTES\tILM.POLICY\tPHASE\tBACKING_DATA_STREAM" > "${report_file}"

jq -r '.[] | [
    .index,
    (.["store.size"] // 0),
    (.["pri.store.size"] // 0),
    (.["ilm.policy"] // "unmanaged")
  ] | @tsv' "${indices_file}" |
while IFS=$'\t' read -r idx store pri policy; do
  # Get accurate phase from _ilm/explain
  phase="not managed"
  ilm_resp=$(curl -s -u "${ES_USER}:${ES_PASS}" "${ES_URL}/${idx}/_ilm/explain?human" 2>/dev/null || echo "")
  if [[ -n "${ilm_resp}" ]]; then
    policy=$(echo "${ilm_resp}" | jq -r '.indices."'"${idx}"'".policy // "unmanaged"' 2>/dev/null || echo "unmanaged")
    phase=$(echo "${ilm_resp}" | jq -r '.indices."'"${idx}"'".phase // "not managed"' 2>/dev/null || echo "not managed")
  fi

  # Infer BACKING_DATA_STREAM from index name pattern
  backing_ds="-"

  if [[ "$idx" == .ds-* ]]; then
    # Pattern: .ds-logs-prod-2025.03.11-000001 → logs-prod
    backing_ds=$(echo "$idx" | sed 's/^\.ds-//; s/-[0-9]\{4\}\.[0-9]\{2\}\.[0-9]\{2\}-[0-9]\+$//')
  elif [[ "$idx" =~ ^(logs|metrics|traces|audit|synthetics|security|fleet|filebeat|winlogbeat|heartbeat)- ]]; then
    # Common Elastic integration prefixes - take first two parts
    backing_ds=$(echo "$idx" | cut -d'-' -f1-2)
  elif [[ "$idx" =~ ^[a-zA-Z0-9-]+\-[0-9]{4}\.[0-9]{2}\.[0-9]{2} ]]; then
    # Generic date-based pattern: myapp-2025.03.11 → myapp
    backing_ds=$(echo "$idx" | sed 's/-[0-9]\{4\}\.[0-9]\{2\}\.[0-9]\{2\}.*//')
  fi

  echo -e "$idx\t$store\t$pri\t$policy\t$phase\t$backing_ds" >> "${report_file}"
done

# Display final table
echo -e "\n=== FINAL REPORT (only indices) ==="
tail -n +2 "${report_file}" | sort -k1 | \
  (echo -e "INDEX\tSTORE_BYTES\tPRI_STORE_BYTES\tILM.POLICY\tPHASE\tBACKING_DATA_STREAM"; cat -) | \
  column -t -s $'\t'

echo -e "\nReport saved to: ${report_file}"

# ───────────────────────────────────────────────────────────────
echo -e "\n=== ILM Policies Export ===\n"

read -p "Export ILM policies (JSON + CSV with all phases)? (y/N): " export_choice

if [[ "${export_choice,,}" =~ ^(y|yes)$ ]]; then
  timestamp=$(date +"%Y-%m-%d_%H-%M-%S")
  json_file="ilm_policies_export_${timestamp}.json"
  csv_file="ilm_policies_export_${timestamp}_all_phases.csv"
  tmp_file="${csv_file}.tmp"

  echo "Fetching all ILM policies..."
  curl -s -u "${ES_USER}:${ES_PASS}" \
    -o "${json_file}" \
    "${ES_URL}/_ilm/policy?pretty&human"

  echo "DEBUG: Policies JSON size: $(wc -c < "${json_file}") bytes"

  if [ ! -s "${json_file}" ]; then
    echo "ERROR: Failed to fetch policies"
    exit 1
  fi

  # ────────────── CSV with all phases ──────────────
  echo "Generating CSV with all phases (no usage info)..."

  jq -r '
    to_entries[]
    | .key as $name
    | .value // {}
    | .policy // {}
    | .phases // {}
    | [
        $name,
        (.version // "null"),
        (.hot.min_age // "-"),
        (.hot.actions | keys | join(", ") // "-"),
        (.warm.min_age // "-"),
        (.warm.actions | keys | join(", ") // "-"),
        (.cold.min_age // "-"),
        (.cold.actions | keys | join(", ") // "-"),
        (.frozen.min_age // "-"),
        (.frozen.actions | keys | join(", ") // "-"),
        (.delete.min_age // "-"),
        (.delete.actions | keys | join(", ") // "-")
      ] | @csv
  ' "${json_file}" > "${tmp_file}" 2> jq_err.log

  if [ -s jq_err.log ]; then
    echo "jq errors/warnings:"
    cat jq_err.log
  fi

  if [ -f "${tmp_file}" ] && [ -s "${tmp_file}" ]; then
    echo "CSV temp file created with content:"
    wc -l < "${tmp_file}"
    head -n 5 "${tmp_file}"
  else
    echo "CSV temp file missing or empty"
    ls -l "${tmp_file}" 2>/dev/null || echo "(file not created)"
  fi

  # Final CSV with header
  {
    echo "policy_name,version,hot_min_age,hot_actions,warm_min_age,warm_actions,cold_min_age,cold_actions,frozen_min_age,frozen_actions,delete_min_age,delete_actions"
    [ -f "${tmp_file}" ] && cat "${tmp_file}"
  } > "${csv_file}"

  rm -f "${tmp_file}" jq_err.log

  if [ -f "${csv_file}" ] && [ -s "${csv_file}" ]; then
    echo "Success! CSV created: ${csv_file}"
    echo "First few lines:"
    head -n 5 "${csv_file}"
  else
    echo "WARNING: CSV is empty or failed to create"
  fi
else
  echo "Export skipped."
fi

echo -e "\n=== Finished ===\n"

read -p "Remove temporary files (y/N)? " cleanup
if [[ "${cleanup,,}" =~ ^(y|yes)$ ]]; then
  rm -f sizes.json report.tsv ilm_policies_export_* 2>/dev/null
  echo "Temporary files removed."
fi
