#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Elasticsearch ILM Report + Policies Export - FIXED VERSION
# - Only indices in report
# - Correct sizes, phase, and BACKING_DATA_STREAM
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
echo -e "\nFetching indices with sizes and ILM info..."

indices_file="indices.json"
curl -s -u "${ES_USER}:${ES_PASS}" \
  "${ES_URL}/_cat/indices?format=json&h=index,store.size,pri.store.size,ilm.policy" > "${indices_file}"

if [ ! -s "${indices_file}" ]; then
  echo "ERROR: Failed to fetch indices"
  exit 1
fi

echo -e "\nFetching data streams for backing info..."

ds_file="datastreams.json"
curl -s -u "${ES_USER}:${ES_PASS}" \
  "${ES_URL}/_data_stream?pretty" > "${ds_file}"

# ───────────────────────────────────────────────────────────────
echo -e "\nBuilding report (only indices + backing data stream)..."

report_file="report.tsv"

# Header
echo -e "INDEX\tSTORE_BYTES\tPRI_STORE_BYTES\tILM.POLICY\tPHASE\tBACKING_DATA_STREAM" > "${report_file}"

# Process every index
jq -r '.[] | [.index, .["store.size"] // "-", .["pri.store.size"] // "-", (.["ilm.policy"] // "unmanaged") ] | @tsv' "${indices_file}" |
while IFS=$'\t' read -r idx store pri policy; do
  # Get accurate phase from _ilm/explain
  phase="not managed"
  ilm_resp=$(curl -s -u "${ES_USER}:${ES_PASS}" "${ES_URL}/${idx}/_ilm/explain?human" 2>/dev/null || echo "")
  if [[ -n "${ilm_resp}" ]]; then
    phase=$(echo "${ilm_resp}" | jq -r '.indices."'"${idx}"'".phase // "not managed"' 2>/dev/null || echo "not managed")
  fi

  # Find backing data stream (exact membership check)
  backing_ds=$(jq -r --arg idx "$idx" '
    .data_streams[]
    | select(.indices[]? == $idx)
    | .name
  ' "${ds_file}" | head -n 1)

  [[ -z "$backing_ds" ]] && backing_ds="-"

  echo -e "$idx\t$store\t$pri\t$policy\t$phase\t$backing_ds" >> "${report_file}"
done

# Display nice table
echo -e "\n=== FINAL REPORT (indices only) ==="
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

  # Usage info
  curl -s -u "${ES_USER}:${ES_PASS}" \
    "${ES_URL}/_cat/indices?format=json&h=index,ilm.policy" > indices_usage.json

  curl -s -u "${ES_USER}:${ES_PASS}" \
    "${ES_URL}/_data_stream?pretty" > datastreams_usage.json

  echo "Generating focused CSV..."
  jq -r --slurpfile policies "${json_file}" --slurpfile ind indices_usage.json --slurpfile ds datastreams_usage.json '
    $policies[0] as $pols
    | $ind[0] as $inds
    | $ds[0].data_streams as $dstreams
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
        indices_list: ($inds | map(select(.["ilm.policy"] == $name) | .index) | join(", ") | if length > 200 then .[0:197]+"..." else . end // "-"),
        ds_count: ($dstreams | map(select(.ilm_policy == $name)) | length),
        ds_list: ($dstreams | map(select(.ilm_policy == $name) | .name) | join(", ") | if length > 200 then .[0:197]+"..." else . end // "-")
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
        .ds_count,
        .indices_list,
        .ds_list
      ] | @csv
  ' "${json_file}" > "${tmp_file}" 2> jq_err.log

  if [ -s jq_err.log ]; then
    echo "jq errors:"
    cat jq_err.log
  fi

  {
    echo "policy_name,version,hot_max_age,hot_max_size,hot_max_primary_shard_size,hot_max_docs,cold_min_age,cold_actions,delete_min_age,delete_actions,used_by_indices_count,used_by_data_streams_count,used_by_indices_list,used_by_data_streams_list"
    [ -f "${tmp_file}" ] && cat "${tmp_file}"
  } > "${csv_file}"

  rm -f "${tmp_file}" jq_err.log indices_usage.json datastreams_usage.json

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
  rm -f sizes.json report.tsv ilm_policies_export_* indices.json datastreams.json 2>/dev/null
  echo "Temporary files removed."
fi
