#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Elasticsearch ILM Report + Policies Export - FINAL with in-use-by
# =============================================================================

echo "Script started at $(date)"

read -p "Elasticsearch username: " ES_USER
read -s -p "Elasticsearch password: " ES_PASS
echo -e "\n"

# === Configuration ===
ES_PROTO="http"                    # change to "https" if needed
ES_HOST="localhost:9200"           # host + port only (no protocol)
ES_URL="${ES_PROTO}://${ES_HOST}"

echo "=== Connecting to: ${ES_URL} ==="

# ───────────────────────────────────────────────────────────────
echo -e "\n[1/2] Fetching index sizes..."

sizes_file="sizes.json"
curl_http=$(curl -s -u "${ES_USER}:${ES_PASS}" \
  -o "${sizes_file}" -w "%{http_code}" \
  "${ES_URL}/_cat/indices?format=json&h=index,store.size,pri.store.size&bytes=b&expand_wildcards=open")

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
  ilm_resp=$(curl -s -u "${ES_USER}:${ES_PASS}" "${ES_URL}/${idx}/_ilm/explain?human" 2>/dev/null || echo "")

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

read -p "Export ILM policies (JSON + CSV with separated hot rollover + in-use-by)? (y/N): " export_choice

if [[ "${export_choice,,}" =~ ^(y|yes)$ ]]; then
  timestamp=$(date +"%Y-%m-%d_%H-%M-%S")
  json_file="ilm_policies_export_${timestamp}.json"
  csv_file="ilm_policies_export_${timestamp}_focused.csv"
  tmp_file="${csv_file}.tmp"

  echo "Fetching policies..."
  curl_http=$(curl -s -u "${ES_USER}:${ES_PASS}" \
    -o "${json_file}" -w "%{http_code}" \
    "${ES_URL}/_ilm/policy?pretty&human")

  echo "DEBUG: HTTP status: ${curl_http}"
  echo "DEBUG: JSON file size: $( [ -f "${json_file}" ] && wc -c < "${json_file}" || echo "0" ) bytes"

  if [ "${curl_http}" -ne 200 ] || [ ! -f "${json_file}" ] || [ ! -s "${json_file}" ]; then
    echo "ERROR: Failed to fetch policies (HTTP ${curl_http})"
    [ -f "${json_file}" ] && head -n 10 "${json_file}"
    exit 1
  fi

  policy_count=$(jq 'length' "${json_file}" 2>/dev/null || echo "0")
  echo "DEBUG: Found ${policy_count} policies"

  # ────────────── Fetch usage info ──────────────
  echo "Collecting usage info (indices & data streams)..."

  curl -s -u "${ES_USER}:${ES_PASS}" \
    "${ES_URL}/_cat/indices?format=json&h=index,ilm.policy" > indices_usage.json

  curl -s -u "${ES_USER}:${ES_PASS}" \
    "${ES_URL}/_data_stream?pretty" > datastreams_usage.json

  # ────────────── Focused CSV ──────────────
  echo "Generating focused CSV..."

  jq -r --slurpfile policies "${json_file}" --slurpfile ind indices_usage.json --slurpfile ds datastreams_usage.json '
    $policies[0] as $pols
    | $ind[0] as $inds
    | $ds[0].data_streams as $dstreams

    | $pols | to_entries[]
    | .key as $name
    | .value // {}
    | .policy.phases.hot.actions.rollover as $r
    | {
        name: $name,
        version: (.version // "null"),
        hot_max_age: ($r.max_age // "-"),
        hot_max_size: ($r.max_size // "-"),
        hot_max_primary_shard_size: ($r.max_primary_shard_size // "-"),
        hot_max_docs: ($r.max_docs // "-"),
        cold_min_age: (.policy.phases.cold.min_age // "-"),
        cold_actions: (.policy.phases.cold.actions | keys | join(", ") // "-"),
        delete_min_age: (.policy.phases.delete.min_age // "-"),
        delete_actions: (.policy.phases.delete.actions | keys | join(", ") // "-"),
        indices_count: ($inds | map(select(.["ilm.policy"] == $name)) | length),
        indices_list: ($inds | map(select(.["ilm.policy"] == $name) | .index) | join(", ") | if length > 200 then .[0:197]+"..." else . end),
        ds_count: ($dstreams | map(select(.ilm_policy == $name)) | length),
        ds_list: ($dstreams | map(select(.ilm_policy == $name) | .name) | join(", ") | if length > 200 then .[0:197]+"..." else . end)
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
  rm -f sizes.json report.tsv ilm_policies_export_* 2>/dev/null
  echo "Temporary files removed."
fi
