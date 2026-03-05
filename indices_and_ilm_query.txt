#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Elasticsearch ILM Report + Policies Export (JSON + Focused CSV) - Robust v3
# =============================================================================

echo "Script started at $(date)"

read -p "Elasticsearch username: " ES_USER
read -s -p "Elasticsearch password: " ES_PASS
echo -e "\n"

ES_PROTO="http"                 # change to "https" if needed
ES_HOST="${ES_PROTO}://localhost:9200"

echo "=== Connecting to: ${ES_HOST} ==="

# ───────────────────────────────────────────────────────────────
echo -e "\n[1/2] Fetching index sizes..."

sizes_http=$(curl -s -u "${ES_USER}:${ES_PASS}" \
  -o sizes.json -w "%{http_code}" \
  "${ES_HOST}/_cat/indices?format=json&h=index,store.size,pri.store.size&bytes=b&expand_wildcards=open")

echo "Sizes fetch HTTP status: ${sizes_http}"

if [ "${sizes_http}" -ne 200 ] || [ ! -s sizes.json ]; then
  echo "ERROR: Failed to fetch index sizes (HTTP ${sizes_http})"
  head -n 10 sizes.json 2>/dev/null || echo "(empty)"
  exit 1
fi

echo "sizes.json size: $(wc -c < sizes.json) bytes"

# ───────────────────────────────────────────────────────────────
echo -e "\n[2/2] Building index + ILM report..."

echo -e "INDEX\tSTORE_BYTES\tPRI_STORE_BYTES\tILM.POLICY\tPHASE" > report.tsv

jq -r '.[] | [.index, .["store.size"] // 0, .["pri.store.size"] // 0] | @tsv' sizes.json |
while IFS=$'\t' read -r idx total pri; do
  ilm_resp=$(curl -s -u "${ES_USER}:${ES_PASS}" "${ES_HOST}/${idx}/_ilm/explain?human" 2>/dev/null)

  if [[ -z "${ilm_resp}" ]]; then
    policy="unmanaged"
    phase="(fetch failed)"
  else
    policy=$(echo "${ilm_resp}" | jq -r '.indices."'"${idx}"'".policy // "unmanaged"')
    phase=$(echo "${ilm_resp}" | jq -r '.indices."'"${idx}"'".phase // (if .indices."'"${idx}"'".managed // false then "managed-no-phase" else "not managed" end)')
  fi

  echo -e "${idx}\t${total}\t${pri}\t${policy}\t${phase}" >> report.tsv
done

tail -n +2 report.tsv | sort -k2 -n -r | (echo -e "INDEX\tSTORE_BYTES\tPRI_STORE_BYTES\tILM.POLICY\tPHASE"; cat -) | column -t -s $'\t'

echo -e "\nIndex report complete."

# ───────────────────────────────────────────────────────────────
echo -e "\n=== ILM Policies Export ===\n"

read -p "Export all ILM policies (JSON + focused CSV)? (y/N): " export_choice

if [[ "${export_choice,,}" =~ ^(y|yes)$ ]]; then
  timestamp=$(date +"%Y-%m-%d_%H-%M-%S")
  json_file="ilm_policies_export_${timestamp}.json"
  csv_file="ilm_policies_export_${timestamp}_focused.csv"

  echo "Fetching policies..."
  curl_http=$(curl -s -u "${ES_USER}:${ES_PASS}" \
    -o "${json_file}" -w "%{http_code}" \
    "${ES_HOST}/_ilm/policy?pretty&human")

  echo "DEBUG: HTTP status: $curl_http"
  echo "DEBUG: File size: $( [ -f "$json_file" ] && wc -c < "$json_file" || echo "0" ) bytes"

  if [ "$curl_http" -ne 200 ] || [ ! -s "$json_file" ]; then
    echo "ERROR: Failed to fetch policies"
    exit 1
  fi

  policy_count=$(jq 'length' "${json_file}" 2>/dev/null || echo "0")
  echo "DEBUG: Found $policy_count policies"

  # Temporary minimal dump to see if ANY policy is readable
  echo "DEBUG: Attempting minimal dump of first policy..."
  jq 'to_entries[0] // "NO ENTRIES OR INVALID TOP LEVEL"' "${json_file}"

  # Very minimal CSV – only names + version
  echo "policy_name,version" > "${csv_file}.minimal.csv"
  jq -r 'to_entries[] | [.key, (.value.version // "null")] | @csv' "${json_file}" >> "${csv_file}.minimal.csv" 2>/dev/null || echo "Minimal jq also failed"

  echo "Minimal CSV created (only name + version): ${csv_file}.minimal.csv"
  head -n 10 "${csv_file}.minimal.csv"

  # Show broken policies for diagnostics
  echo "DEBUG: Checking for broken/null policies..."
  jq -r 'to_entries[] | select(.value == null or .value.policy == null or .value.policy.phases == null) | .key + " (broken)"' "${json_file}" || echo "(none found)"

  # ────────────── Generate CSV ──────────────
  echo "Generating focused CSV..."

  jq -r '
    to_entries[]
    | .key as $name
    | .value // {policy: null}
    | .policy // {phases: {}}
    | {
        name: $name,
        version: (.version // "null"),
        modified: (.modified_date // "null"),
        hot_rollover: (.phases.hot?.actions?.rollover | tojson // "none"),
        cold_age: (.phases.cold?.min_age // "-"),
        cold_actions: (.phases.cold?.actions | keys | join(", ") // "-"),
        delete_age: (.phases.delete?.min_age // "-"),
        delete_actions: (.phases.delete?.actions | keys | join(", ") // "-")
      }
    | [
        .name,
        .version,
        .modified,
        .hot_rollover,
        .cold_age,
        .cold_actions,
        .delete_age,
        .delete_actions
      ] | @csv
  ' "${json_file}" > "${csv_file}.tmp" 2> jq_err.log

  if [ -s jq_err.log ]; then
    echo "jq errors/warnings:"
    cat jq_err.log
  fi

  row_count=$(wc -l < "${csv_file}.tmp" 2>/dev/null || echo 0)
  echo "DEBUG: CSV rows produced (excluding header): $row_count"

  # Add header
  {
    echo "policy_name,version,modified_date,hot_rollover_conditions,cold_phase_min_age,cold_phase_actions,delete_phase_min_age,delete_phase_actions"
    cat "${csv_file}.tmp"
  } > "${csv_file}"

  rm -f "${csv_file}.tmp" jq_err.log

  echo ""
  echo "Export complete:"
  echo "  • Full JSON: ${json_file}"
  echo "  • Focused CSV: ${csv_file}"
else
  echo "Export skipped."
fi

# ───────────────────────────────────────────────────────────────
echo -e "\n=== Finished ===\n"

read -p "Remove temporary files (y/N)? " cleanup
if [[ "${cleanup,,}" =~ ^(y|yes)$ ]]; then
  rm -f sizes.json report.tsv ilm_policies_export_* 2>/dev/null
  echo "Temporary files removed."
fi
