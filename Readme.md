# ASGard

LLM classification of ASNs based on RDAP metadata.

To use the default gpt-5-nano put in your OpenAI `KEY` and specify `N` ASNs to fetch:
```
./gradlew run --args="--api-key=KEY --limit=N"
```

OpenAI request/response payloads are logged automatically under `~/.asgard/logs/{timestamp}/asn-{asn}.json`.
This will output a TSV file at the path from `--output` (defaults to an `.asgard` directory under your home dir).
 - If you omit the N it will run through all ~120k which will use about $5 of open AI credits with the default model. 
A registry cache (see below) will stop it making lookups to the registry for each.
 - If you want the reasoning for any responses, you can reprocess it and show this with `--reprocess=1,3,5`

Registry cache lifecycle:
- If `~/.asgard/cache/registry/registry-cache.ndjson` is missing, the app automatically downloads delegated data from all five RIRs plus ARIN bulk whois (prompts once for an ARIN API key, or uses `--arin-api-key`/env vars, then saves it in `asgard.conf`), builds the cache, and proceeds.
- If the cache exists, it is reused. Pass `--refresh-registry-db` to rebuild it end-to-end and replace the existing file.
- Intermediate artifacts are kept in a temp directory during the build and removed after the final cache file is written.
- `--refresh-allocations` downloads a fresh ASN allocation list into `~/.asgard/cache/allocations`; otherwise the most recent cached file is reused.