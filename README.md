# elasticsearch_query

Shell helpers for Elasticsearch index + ILM reporting, plus sample APM and query notes.

## Scripts

| File | Purpose |
| --- | --- |
| `indices_and_ilm_query_v3.sh` | Latest. Builds a TSV of index store size, ILM policy, phase, and inferred backing data stream. Optionally exports all ILM policies to JSON + CSV. |
| `indices_and_ilm_query_v2.sh` | Previous version. |
| `indices_and_ilm_query_v1.sh` | First version. |
| `debug.sh` | Small debug helper. |
| `apm_transaction_count.txt` | APM transaction count query notes. |
| `qyery_sample.txt` | Sample queries (filename spelling is original). |

Prefer `indices_and_ilm_query_v3.sh`.

## v3 usage

```bash
bash indices_and_ilm_query_v3.sh
```

Prompts for Elasticsearch username/password. Default URL is `http://localhost:9200` (edit `ES_PROTO` / `ES_HOST` in the script for HTTPS).

Outputs:

- `indices.json`
- `report.tsv` (`INDEX`, `STORE_BYTES`, `PRI_STORE_BYTES`, `ILM.POLICY`, `PHASE`, `BACKING_DATA_STREAM`)
- optional `ilm_policies_export_<timestamp>.json` and `_all_phases.csv`

Requires `curl` and `jq`.
