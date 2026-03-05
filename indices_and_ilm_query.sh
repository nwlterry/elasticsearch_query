#!/usr/bin/env bash
set -euo pipefail

echo "Script started at $(date)"

read -p "Elasticsearch username: " ES_USER
read -s -p "Elasticsearch password: " ES_PASS
echo -e "\n"

ES_HOST="https://localhost:9200"   # ← change to https:// if needed

# ───────────────────────────────────────────────────────────────
echo -e "\n[1/2] Fetching index sizes..."

sizes_file="sizes.json"
curl_http=$(curl -s -u "${ES_USER}:${ES_PASS}" \
  -o "${sizes_file}" -w "%{http_code}" \
  "${ES_HOST}/_cat/indices?format=json&h=index,store.size,pri.store.size&bytes=b&expand_wildcards=open")

echo "Sizes HTTP status: ${curl_http}"

if [ "${curl_http}" -ne 200 ] || [ ! -s "${sizes_file}" ]; then
  echo "ERROR: Failed to fetch sizes (HTTP ${curl_http})"
  [ -f "${sizes_file}" ] && head -n 10 "${sizes_file}"
  exit 1
fi

echo "sizes.json size: $(wc -c < "${sizes_file}") bytes"

# ───────────────────────────────────────────────────────────────
echo -e "\n[2/2] Building index report..."

report_file="report.tsv"
echo -e "INDEX\tSTORE_BYTES\tPRI_STORE_BYTES\tILM.POLICY\tPHASE" > "${report_file}"

jq -r '.[] | [.index, .["store.size"] // 0, .["pri.store.size"] // 0] | @tsv' "${sizes_file}" |
while IFS=$'\t' read -r idx total pri; do
  ilm_resp=$(curl -s -u "${ES_USER}:${ES_PASS}" "${ES_HOST}/${idx}/_ilm/explain?human" 2>/dev/null || echo "")

  policy="unmanaged"
  phase="not managed"

  if [[ -n "${ilm_resp}" ]]; then
    policy=$(echo "${ilm_resp}" | jq -r '.indices."'"${idx}"'".policy // "unmanaged"' 2>/dev/null || echo "unmanaged")
    phase=$(echo "${ilm_resp}" | jq -r '.indices."'"${idx}"'".phase // "not managed"' 2>/dev/null || echo "not managed")
  fi

  echo -e "${idx}\t${total}\t${pri}\t${policy}\t${phase}" >> "${report_file}"
done

tail -n +2 "${report_file}" | sort -k2 -n -r | (echo -e "INDEX\tSTORE_BYTES\tPRI_STORE_BYTES\tILM.POLICY\tPHASE"; cat -) | column -t -s $'\t'

echo -e "\nIndex report complete."

# ───────────────────────────────────────────────────────────────
echo -e "\n=== ILM Policies Export ===\n"

read -p "Export ILM policies (JSON + CSV)? (y/N): " export_choice

if [[ "${export_choice,,}" =~ ^(y|yes)$ ]]; then
  timestamp=$(date +"%Y-%m-%d_%H-%M-%S")
  json_file="ilm_policies_export_${timestamp}.json"
  csv_file="ilm_policies_export_${timestamp}_focused.csv"
  tmp_file="${csv_file}.tmp"

  echo "Fetching policies..."
  curl_http=$(curl -s -u "${ES_USER}:${ES_PASS}" \
    -o "${json_file}" -w "%{http_code}" \
    "${ES_HOST}/_ilm/policy?pretty&human")

  echo "DEBUG: HTTP status: ${curl_http}"
  echo "DEBUG: JSON file: ${json_file} ($( [ -f "${json_file}" ] && wc -c < "${json_file}" || echo "0" ) bytes)"

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
    echo "CRITICAL: JSON file missing before jq: ${json_file}"
    ls -l ilm_policies_export_*.json 2>/dev/null || echo "(no files)"
    exit 1
  fi

  echo "DEBUG: jq input  = ${json_file}"
  echo "DEBUG: jq output = ${tmp_file}"

  # Show the EXACT command being run
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
  ' "${json_file}" > "${tmp_file}" 2> jq_err.log
  set +x

  if [ -s jq_err.log ]; then
    echo "jq errors/warnings:"
    cat jq_err.log
  else
    echo "jq ran without stderr"
  fi

  if [ -f "${tmp_file}" ] && [ -s "${tmp_file}" ]; then
    echo "CSV temp file created with content:"
    wc -l < "${tmp_file}"
    head -n 5 "${tmp_file}"
  else
    echo "CSV temp file missing or empty:"
    ls -l "${tmp_file}" 2>/dev/null || echo "(not created)"
  fi

  # Final CSV
  {
    echo "policy_name,version,hot_rollover_conditions,cold_phase_min_age,cold_phase_actions,delete_phase_min_age,delete_phase_actions"
    [ -f "${tmp_file}" ] && cat "${tmp_file}"
  } > "${csv_file}"

  rm -f "${tmp_file}" jq_err.log

  if [ -f "${csv_file}" ] && [ -s "${csv_file}" ]; then
    echo "Success! CSV created: ${csv_file}"
    head -n 5 "${csv_file}"
  else
    echo "WARNING: Final CSV is empty or missing"
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
