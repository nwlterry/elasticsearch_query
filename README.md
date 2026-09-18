# elasticsearch_query

Shell helpers for Elasticsearch index + ILM reporting, plus sample APM/query notes.

## Layout

```
scripts/indices_and_ilm_query.sh   # current (was v3)
scripts/debug.sh
archive/                           # v1, v2
queries/                           # sample queries
GROUP.md
README.md
```

```bash
bash scripts/indices_and_ilm_query.sh
```

Prompts for username/password. Default URL is `http://localhost:9200` (edit `ES_PROTO` / `ES_HOST` in the script for HTTPS). Optional ILM policy JSON + CSV export. Requires `curl` and `jq`.

---

See [GROUP.md](GROUP.md) for sibling repositories. Catalog: https://github.com/nwlterry/nwlterry
