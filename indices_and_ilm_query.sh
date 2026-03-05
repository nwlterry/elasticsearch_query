#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Elasticsearch ILM Report + Policies Export - FINAL SAFE VERSION
# =============================================================================

echo "Script started at $(date)"

read -p "Elasticsearch username: " ES_USER
read -s -p "Elasticsearch password: " ES_PASS
echo -e "\n"

ES_PROTO="http"                 # change to "https" if needed
ES_HOST="${ES_PROTO}://localhost:9200"

echo "=== Connecting to: ${ES_HOST} ==="

# Fetch index sizes
echo -e "\n[1/2] Fetching index sizes..."
sizes_http=$(curl -s -u "${ES_USER}:${ES_PASS}" \
  -o sizes.json -w "%{http_code}" \
  "${ES_HOST}/_cat/indices?format=json&h=index,store.size,pri.store.size&bytes=b&expand_wildcards=open")

echo "Sizes HTTP status: ${sizes_http}"

if [ "${sizes_http}" -ne 200 ] || [ ! -s sizes.json ]; then
  echo "ERROR: Failed to fetch index sizes (HTTP ${sizes_http})"
  [ -f sizes.json ] && head -n 10 sizes.json
  exit 1
fi

echo "sizes.json size: $(wc -c < sizes.json) bytes"

# Build report
echo -e "\n[2/2] Building index + ILM report..."
echo -e "INDEX\tSTORE_BYTES\tPRI_STORE_BYTES\tILM.POLICY\tPHASE" > report.tsv

jq -r '.[] | [.index, .["store.size"] // 0, .["pri.store.size"] // 0] | @tsv' sizes.json |
while IFS=$'\t' read -r idx total pri; do
  ilm_resp=$(curl -s -u "${ES_USER}:${ES_PASS}" "${ES_HOST}/${idx}/_ilm/explain?human" 2>/dev/null || echo "")

  policy="unmanaged"
  phase="not managed"

  if [[ -n "${ilm_resp}" ]]; then
    policy=$(echo "${ilm_resp}" | jq -r '.indices."'"${idx}"'".policy // "unmanaged"' 2>/dev/null || echo "unmanaged")
    phase=$(echo "${ilm_resp}" | jq -r '.indices."'"${idx}"'".phase // "not managed"' 2>/dev/null || echo "not managed")
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
  tmp_csv="${csv_file}.tmp"

  echo "Fetching policies..."
  curl_http=$(curl -s -u "${ES_USER}:${ES_PASS}" \
    -o "${json_file}" -w "%{http_code}" \
    "${ES_HOST}/_ilm/policy?pretty&human")

  echo "DEBUG: HTTP status: ${curl_http}"
  echo "DEBUG: File size: $( [ -f "${json_file}" ] && wc -c < "${json_file}" || echo "0" ) bytes"

  if [ "${curl_http}" -ne 200 ] || [ ! -f "${json_file}" ] || [ ! -s "${json_file}" ]; then
    echo "ERROR: Failed to fetch policies (HTTP ${curl_http})"
    [ -f "${json_file}" ] && head -n 10 "${json_file}"
    exit 1
  fi

  policy_count=$(jq 'length' "${json_file}" 2>/dev/null || echo "0")
  echo "DEBUG: Found ${policy_count} policies"

  # ────────────── Focused CSV ──────────────
  echo "Generating focused CSV..."

  if [ ! -f "${json_file}" ]; then
    echo "ERROR: JSON file disappeared: ${json_file}"
    ls -l ilm_policies_export_*.json 2>/dev/null || echo "(no files)"
    exit 1
  fi

  echo "DEBUG: Running jq on: ${json_file}"
  echo "DEBUG: Output will go to: ${tmp_csv}"

  # Enable shell tracing for this block only
  set -x
  jq -r '
    to_entries[]
    | .key as $name
    | .value // {}
    | [
        $name,
        (.version // "null"),
        (if .policy.phases.hot.actions.rollover? then .policy.phases.hot.actions.rollover | tojson else "none" end),
        (if .policy.phases.cold.min_age? then .policy.phases.cold.min_age else "-" end),
        (if .policy.phases.cold.actions? then (.policy.phases.cold.actions | keys | join(", ")) else "-" end),
        (if .policy.phases.delete.min_age? then .policy.phases.delete.min_age else "-" end),
        (if .policy.phases.delete.actions? then (.policy.phases.delete.actions | keys | join(", ")) else "-" end)
      ] | @csv
  ' "${json_file}" > "${tmp_csv}" 2> jq_err.log
  set +x

  if [ -s jq_err.log ]; then
    echo "jq errors/warnings:"
    cat jq_err.log
  fi

  if [ -f "${tmp_csv}" ] && [ -s "${tmp_csv}" ]; then
    echo "CSV temp file created successfully"
    wc -l < "${tmp_csv}"
    head -n 5 "${tmp_csv}"
  else
    echo "CSV temp file is missing or empty"
    ls -l "${tmp_csv}" 2>/dev/null || echo "(file not created)"
  fi

  # Final file
  {
    echo "policy_name,version,hot_rollover_conditions,cold_phase_min_age,cold_phase_actions,delete_phase_min_age,delete_phase_actions"
    [ -f "${tmp_csv}" ] && cat "${tmp_csv}"
  } > "${csv_file}"

  rm -f "${tmp_csv}" jq_err.log

  if [ -f "${csv_file}" ] && [ -s "${csv_file}" ]; then
    echo "Success! Focused CSV: ${csv_file}"
    head -n 5 "${csv_file}"
  else
    echo "WARNING: Focused CSV failed to create or is empty"
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
