#!/usr/bin/env bash
set -euo pipefail

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
indices_headers="indices_headers.txt"

curl_http=$(curl -s -u "${ES_USER}:${ES_PASS}" \
  -o "${indices_file}" \
  -D "${indices_headers}" \
  -w "%{http_code}" \
  "${ES_URL}/_cat/indices?format=json&h=index,store.size,pri.store.size,ilm.policy,ilm.phase&bytes=b")

echo "HTTP status: ${curl_http}"

if [ "${curl_http}" -ne 200 ]; then
  echo "ERROR: API returned HTTP ${curl_http}"
  echo "Headers:"
  cat "${indices_headers}"
  echo "Response body (first 10 lines):"
  head -n 10 "${indices_file}"
  exit 1
fi

if [ ! -s "${indices_file}" ]; then
  echo "ERROR: indices.json is empty"
  exit 1
fi

jq_type=$(jq type "${indices_file}" 2>/dev/null || echo "invalid")
if [ "${jq_type}" != '"array"' ]; then
  echo "ERROR: Response is not a JSON array (type = ${jq_type})"
  echo "First 300 chars:"
  head -c 300 "${indices_file}" | cat -v
  exit 1
fi

echo "indices.json size: $(wc -c < "${indices_file}") bytes"

# ───────────────────────────────────────────────────────────────
echo -e "\nBuilding report (only indices)..."

report_file="report.tsv"

echo -e "INDEX\tSTORE_BYTES\tPRI_STORE_BYTES\tILM.POLICY\tPHASE\tBACKING_DATA_STREAM" > "${report_file}"

jq -r '.[] | [
    .index,
    (.["store.size"] // 0),
    (.["pri.store.size"] // 0),
    (.["ilm.policy"] // "unmanaged"),
    (.["ilm.phase"] // "not managed"),
    "-"   # BACKING_DATA_STREAM placeholder (can add later if needed)
  ] | @tsv' "${indices_file}" >> "${report_file}"

# Display
echo -e "\n=== FINAL REPORT (only indices) ==="
tail -n +2 "${report_file}" | sort -k1 | \
  (echo -e "INDEX\tSTORE_BYTES\tPRI_STORE_BYTES\tILM.POLICY\tPHASE\tBACKING_DATA_STREAM"; cat -) | \
  column -t -s $'\t'

echo -e "\nReport saved to: ${report_file}"

# ───────────────────────────────────────────────────────────────
echo -e "\n=== ILM Policies Export ===\n"

read -p "Export ILM policies (JSON + CSV)? (y/N): " export_choice

if [[ "${export_choice,,}" =~ ^(y|yes)$ ]]; then
  timestamp=$(date +"%Y-%m-%d_%H-%M-%S")
  json_file="ilm_policies_export_${timestamp}.json"
  csv_file="ilm_policies_export_${timestamp}_focused.csv"

  echo "Fetching policies..."
  curl -s -u "${ES_USER}:${ES_PASS}" \
    -o "${json_file}" \
    "${ES_URL}/_ilm/policy?pretty&human"

  if [ ! -s "${json_file}" ]; then
    echo "ERROR: Failed to fetch policies"
    exit 1
  fi

  echo "Policies JSON size: $(wc -c < "${json_file}") bytes"

  # Optional: Fetch usage for CSV (indices only)
  curl -s -u "${ES_USER}:${ES_PASS}" \
    "${ES_URL}/_cat/indices?format=json&h=index,ilm.policy" > indices_usage.json

  # Generate CSV (simplified - add your full jq here)
  echo "Generating focused CSV... (simplified version)"
  jq -r 'to_entries[] | [.key, (.value.version // "null")] | @csv' "${json_file}" > "${csv_file}"

  echo "CSV created: ${csv_file}"
  head -n 5 "${csv_file}"
else
  echo "Export skipped."
fi

echo -e "\n=== Finished ===\n"

read -p "Remove temporary files (y/N)? " cleanup
if [[ "${cleanup,,}" =~ ^(y|yes)$ ]]; then
  rm -f "${indices_file}" "${report_file}" "${json_file}" "${csv_file}" indices_usage.json 2>/dev/null
  echo "Temporary files removed."
fi
