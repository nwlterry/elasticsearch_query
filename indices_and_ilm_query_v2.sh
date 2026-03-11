#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Elasticsearch ILM Report + Policies Export
# - Only regular indices in report (no data stream rows)
# - Added BACKING_DATA_STREAM column (shows owning data stream name if any)
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
echo -e "\nFetching indices..."

indices_file="indices.json"
curl -s -u "${ES_USER}:${ES_PASS}" \
  "${ES_URL}/_cat/indices?format=json&h=index,store.size,pri.store.size,ilm.policy,ilm.phase" > "${indices_file}"

if [ ! -s "${indices_file}" ]; then
  echo "ERROR: Failed to fetch indices"
  exit 1
fi

# ───────────────────────────────────────────────────────────────
echo -e "\nFetching data streams (for backing info)..."

ds_file="datastreams.json"
curl -s -u "${ES_USER}:${ES_PASS}" \
  "${ES_URL}/_data_stream?pretty" > "${ds_file}"

# ───────────────────────────────────────────────────────────────
echo -e "\nBuilding report (only indices + backing data stream info)..."

report_file="report.tsv"

# Header
echo -e "INDEX\tTYPE\tSTORE_BYTES\tPRI_STORE_BYTES\tILM.POLICY\tPHASE\tBACKING_DATA_STREAM" > "${report_file}"

# Add indices
jq -r '.[] | [
    .index,
    "index",
    (.store_size // "-"),
    (.pri_store_size // "-"),
    (.["ilm.policy"] // "unmanaged"),
    (.["ilm.phase"] // "not managed"),
    "-"   # placeholder for backing DS - filled later
  ] | @tsv' "${indices_file}" >> "${report_file}"

# ───────────────────────────────────────────────────────────────
echo "Matching indices to data streams..."

# Create a map of data stream → backing indices pattern (simplified)
# We check if index name matches any data stream backing index pattern
# (real matching requires parsing .indices in _data_stream, but for simplicity we use name prefix match)

while IFS=$'\t' read -r idx type store pri policy phase ds_placeholder; do
  # Skip header
  [[ "$idx" == "INDEX" ]] && continue

  backing_ds="-"

  # Try to find if this index belongs to any data stream
  # (simple prefix match - adjust if your DS patterns are different)
  jq -r --arg idx "$idx" '
    .data_streams[]
    | select(.indices[]? | startswith($idx) or ($idx | startswith(.)))
    | .name
  ' "${ds_file}" | while read -r ds_name; do
    backing_ds="$ds_name"
    break  # take first match (most common case)
  done

  echo -e "$idx\t$type\t$store\t$pri\t$policy\t$phase\t$backing_ds" >> "${report_file}.tmp"
done < "${report_file}"

mv "${report_file}.tmp" "${report_file}"

# Display final table
tail -n +2 "${report_file}" | sort -k1 | (echo -e "INDEX\tTYPE\tSTORE_BYTES\tPRI_STORE_BYTES\tILM.POLICY\tPHASE\tBACKING_DATA_STREAM"; cat -) | column -t -s $'\t'

echo -e "\nReport complete (only indices + backing data stream name): ${report_file}"

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

  if [ ! -s "${json_file}" ]; then
    echo "ERROR: Failed to fetch policies"
    exit 1
  fi

  echo "DEBUG: Policies JSON size: $(wc -c < "${json_file}") bytes"

  # Fetch usage (for CSV)
  curl -s -u "${ES_USER}:${ES_PASS}" \
    "${ES_URL}/_cat/indices?format=json&h=index,ilm.policy" > indices_usage.json

  curl -s -u "${ES_USER}:${ES_PASS}" \
    "${ES_URL}/_data_stream?pretty" > datastreams_usage.json

  # Generate CSV
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
    echo "jq errors/warnings:"
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
    echo "WARNING: Focused CSV is empty or failed to create"
  fi
else
  echo "Export skipped."
fi

echo -e "\n=== Finished ===\n"

read -p "Remove temporary files (y/N)? " cleanup
if [[ "${cleanup,,}" =~ ^(y|yes)$ ]]; then
  rm -f "${sizes_file}" "${report_file}" "${json_file}" "${csv_file}" indices.json datastreams.json 2>/dev/null
  echo "Temporary files removed."
fi
