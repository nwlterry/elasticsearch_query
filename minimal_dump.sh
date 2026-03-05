  # Temporary minimal dump to see if ANY policy is readable
  echo "DEBUG: Attempting minimal dump of first policy..."
  jq 'to_entries[0] // "NO ENTRIES OR INVALID TOP LEVEL"' "${json_file}"

  # Very minimal CSV – only names + version
  echo "policy_name,version" > "${csv_file}.minimal.csv"
  jq -r 'to_entries[] | [.key, (.value.version // "null")] | @csv' "${json_file}" >> "${csv_file}.minimal.csv" 2>/dev/null || echo "Minimal jq also failed"

  echo "Minimal CSV created (only name + version): ${csv_file}.minimal.csv"
  head -n 10 "${csv_file}.minimal.csv"
