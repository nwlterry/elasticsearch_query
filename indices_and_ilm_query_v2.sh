#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Elasticsearch ILM Report + Policies Export - UPDATED
# - Sizes in raw bytes
# - Only indices in report
# - BACKING_DATA_STREAM from _cat/indices (no separate _data_stream call)
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
echo -e "\nFetching all indices with sizes (bytes), ILM, and data stream info..."

indices_file="indices.json"
curl -s -u "${ES_USER}:${ES_PASS}" \
  "${ES_URL}/_cat/indices?format=json&h=index,store.size,pri.store.size,ilm.policy,ilm.phase,data_stream&bytes=b" > "${indices_file}"

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

# Process indices
jq -r '.[] | [
    .index,
    (.["store.size"] // 0),
    (.["pri.store.size"] // 0),
    (.["ilm.policy"] // "unmanaged"),
    (.["ilm.phase"] // "not managed"),
    (.["data_stream"] // "-")
  ] | @tsv' "${indices_file}" >> "${report_file}"

# Display final table
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
  tmp_file="${csv_file}.tmp"

  echo "Fetching policies..."
  curl -s -u "${ES_USER}:${ES_PASS}" \
    -o "${json_file}" \
    "${ES_URL}/_ilm/policy?pretty&human"

  echo "DEBUG: Policies JSON size: $(wc -c < "${json_file}") bytes"

  # Usage info (indices only - data streams usage removed since not needed)
  curl -s -u "${ES_USER}:${ES_PASS}" \
    "${ES_URL}/_cat/indices?format=json&h=index,ilm.policy" > indices_usage.json

  # Generate CSV (only indices usage)
  echo "Generating focused CSV..."

  jq -r --slurpfile policies "${json_file}" --slurpfile ind indices_usage.json '
    $policies[0] as $pols
    | $ind[0] as $inds

    | $pols | to_entries[]
    | .key as $name
    | .value // {}
    | .policy // {}
    | .phases // {}
    | .hot // {}
    | .actions // {}
    | .rollover // {}
    | {
        name: $name,
        version: (.version // "null"),
        hot_max_age: (.max_age // "-"),
        hot_max_size: (.max_size // "-"),
        hot_max_primary_shard_size: (.max_primary_shard_size // "-"),
        hot_max_docs: (.max_docs // "-"),
        cold_min_age: (.cold.min_age // "-"),
        cold_actions: (.cold.actions | if type == "object" then keys else [] end | join(", ") // "-"),
        delete_min_age: (.delete.min_age // "-"),
        delete_actions: (.delete.actions | if type == "object" then keys else [] end | join(", ") // "-"),
        indices_count: ($inds | map(select(.["ilm.policy"] == $name)) | length),
        indices_list: ($inds | map(select(.["ilm.policy"] == $name) | .index) | join(", ") | if length > 200 then .[0:197]+"..." else . end // "-")
      }
    | [
        .name,
        .version,
        .hot_max_age,
        .hot_max_size,
        .hot_max_primary_shard_size,
        .hot_max_docs,
        .cold_min_age,
        .cold_actions,
        .delete_min_age,
        .delete_actions,
        .indices_count,
        .indices_list
      ] | @csv
  ' "${json_file}" > "${tmp_file}" 2> jq_err.log

  if [ -s jq_err.log ]; then
    echo "jq errors:"
    cat jq_err.log
  fi

  {
    echo "policy_name,version,hot_max_age,hot_max_size,hot_max_primary_shard_size,hot_max_docs,cold_min_age,cold_actions,delete_min_age,delete_actions,used_by_indices_count,used_by_indices_list"
    [ -f "${tmp_file}" ] && cat "${tmp_file}"
  } > "${csv_file}"

  rm -f "${tmp_file}" jq_err.log indices_usage.json

  if [ -f "${csv_file}" ] && [ -s "${csv_file}" ]; then
    echo "Success! Focused CSV created: ${csv_file}"
    echo "First few lines:"
    head -n 5 "${csv_file}"
  else
    echo "WARNING: Focused CSV is empty"
  fi
else
  echo "Export skipped."
fi

echo -e "\n=== Finished ===\n"

read -p "Remove temporary files (y/N)? " cleanup
if [[ "${cleanup,,}" =~ ^(y|yes)$ ]]; then
  rm -f "${indices_file}" "${report_file}" "${json_file}" "${csv_file}" 2>/dev/null
  echo "Temporary files removed."
fi
