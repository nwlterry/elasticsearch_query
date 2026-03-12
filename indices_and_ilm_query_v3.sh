#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Elasticsearch ILM Report + Policies Export - UPDATED (CSV with all phases)
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
echo -e "\n[1/2] Fetching index sizes (raw bytes) and ILM info..."

sizes_file="sizes.json"
curl -s -u "${ES_USER}:${ES_PASS}" \
  -o "${sizes_file}" -w "%{http_code}" \
  "${ES_URL}/_cat/indices?format=json&h=index,store.size,pri.store.size,ilm.policy,ilm.phase&bytes=b" > "${sizes_file}"

if [ ! -s "${sizes_file}" ]; then
  echo "ERROR: Failed to fetch indices"
  exit 1
fi

echo "sizes.json size: $(wc -c < "${sizes_file}") bytes"

# ───────────────────────────────────────────────────────────────
echo -e "\n[2/2] Building index report..."

report_file="report.tsv"

echo -e "INDEX\tSTORE_BYTES\tPRI_STORE_BYTES\tILM.POLICY\tPHASE\tBACKING_DATA_STREAM" > "${report_file}"

jq -r '.[] | [
    .index,
    (.["store.size"] // 0),
    (.["pri.store.size"] // 0),
    (.["ilm.policy"] // "unmanaged"),
    (.["ilm.phase"] // "not managed"),
    "-"   # BACKING_DATA_STREAM placeholder
  ] | @tsv' "${sizes_file}" |
while IFS=$'\t' read -r idx store pri policy phase ds_placeholder; do
  # Get accurate phase (already from _cat, but can be overridden if needed)
  ilm_resp=$(curl -s -u "${ES_USER}:${ES_PASS}" "${ES_URL}/${idx}/_ilm/explain?human" 2>/dev/null || echo "")
  if [[ -n "${ilm_resp}" ]]; then
    phase=$(echo "${ilm_resp}" | jq -r '.indices."'"${idx}"'".phase // "'"${phase}"'"' 2>/dev/null || echo "${phase}")
  fi

  echo -e "$idx\t$store\t$pri\t$policy\t$phase\t-" >> "${report_file}"
done

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
