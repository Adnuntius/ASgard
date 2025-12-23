# ASgard

LLM classification of ASNs based on RDAP metadata.

To use the default gpt-5-nano put in your OpenAI `KEY` and specify `N` ASNs to fetch:
```
./gradlew run --args="--api-key=KEY --limit=N"
```
- Omit the limit key to process every ASN, with the default model this will use around $7 USD of OpenAI credits.
- Results are written as TSV to `~/.asgard/asn-classifications.tsv` (specify a different path with `--output`).
- OpenAI request/response payloads are logged automatically under `~/.asgard/logs/{timestamp}/asn-{asn}.json`.

## Options

| Flag | Description |
|------|-------------|
| `--api-key=KEY` | OpenAI API key |
| `--arin-api-key=KEY` | ARIN bulk whois API key |
| `--model=MODEL` | OpenAI model (default: gpt-5-nano) |
| `--limit=N` | Max ASNs to classify (default: no limit) |
| `--output=PATH` | Output TSV file path |
| `--reprocess=1,3,5` | Re-classify specific ASNs and show reasoning |
| `--accept-unknowns` | Persist "Unknown" classifications instead of retrying |
| `--skip-arin-bulk` | Skip ARIN bulk download (some ASNs will show "Unknown") |
| `--refresh-registry-db` | Force rebuild of registry cache |
| `--refresh-allocations` | Download fresh ASN allocation list |
| `--tpm-limit=N` | Tokens per minute limit (default: 200000) |
| `--max-context=N` | Max tokens per request (default: 250000) |

## Allocation and Registry Caches

Allocation data is cached in `~/.asgard/cache/allocations/`. Pass `--refresh-allocations` to download fresh data.

The registry cache at `~/.asgard/cache/registry/registry-cache.ndjson` stores ASN metadata from all five RIRs.

- If missing, the app automatically downloads delegated data from all RIRs plus ARIN bulk whois, builds the cache, and proceeds
- If present, it is reused. Pass `--refresh-registry-db` to rebuild
- ARIN API key is saved in `~/.asgard/asgard.conf` after first prompt. The build will fail if your ARIN API key doesn't 
have bulk whois access. If the saved API key fails, it will be removed from config and you'll be prompted for a new one.
- To proceed without ARIN bulk data and use only ARIN's public Internet Routing Registry, add `--skip-arin-bulk`.

<p style="text-align: center;">
  <a href="https://github.com/iondrive-co/chad">
    <img src="https://raw.githubusercontent.com/iondrive-co/chad/main/docs/Chad.png" alt="Chad Code" width="80">
  </a>
</p>
