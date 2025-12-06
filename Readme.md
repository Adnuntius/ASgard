# ASGard

LLM classification of ASNs based on RDAP metadata.

To use the default gpt-5-nano put in your OpenAI `KEY` and specify `N` ASNs to fetch:
```
./gradlew run --args="--api-key=KEY --limit=N"
```
This will output a TSV file at the path from `--output` (defaults to an `.asgard` directory under your home dir).
 - If you omit the N it will run through all ~120k which is not a good idea as each currently requires an RDAP lookup 
 - If you want the reasoning for any responses, you can reprocess it and show this with `--reprocess=1,3,5`
