<p align="center">
  <img src="https://raw.githubusercontent.com/MickaelBZH/data-contract-x/main/assets/logo.svg" alt="dcx — Data Contract eXtended" width="520">
</p>

<h3 align="center">Data Contract e<strong>X</strong>tended — AI-native, platform-extensible data contracts</h3>

<p align="center">
  Author data contracts with an LLM, sync them with your live platforms.<br>
  A lean, no-fork extension of <a href="https://github.com/datacontract/datacontract-cli">datacontract-cli</a>, built on the <a href="https://bitol.io/">Open Data Contract Standard (ODCS)</a>.
</p>

<p align="center">
  <img alt="Python" src="https://img.shields.io/badge/python-3.10%20|%203.11%20|%203.12-3776AB?logo=python&logoColor=white">
  <img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-22c55e">
  <img alt="ODCS" src="https://img.shields.io/badge/ODCS-v3.1-0EA5E9">
  <img alt="Built on datacontract-cli" src="https://img.shields.io/badge/built%20on-datacontract--cli-6366F1">
  <img alt="Tests" src="https://img.shields.io/badge/tests-211%20passing-22c55e">
</p>

---

## What is dcx?

**dcx (Data Contract eXtended)** adds three things to the Open Data Contract Standard workflow that plain datacontract-cli doesn't do:

1. **AI authoring** — use an LLM to enrich a contract with column descriptions, validation constraints, governance **tags** from your own catalog, and an executable **data-quality** suite.
2. **Live import** — build a contract *from* a running system (its real columns, keys, comments, tags).
3. **Apply** — push the contract's governance *back* to the platform (comments, tags, data-quality, and the table itself).

It's **platform-extensible by design**: each platform is a small importer / exporter / apply module that plugs into datacontract-cli's factories. **Snowflake is the first end-to-end platform** (import → enrich → apply), with Kafka import today and more platforms built to slot in the same way.

The pipeline is: **import** a live schema into an ODCS contract → **enrich** it (columns · tags · quality) → **apply** it back to the platform, or **export** it to SQL / docs / schemas. Everything is available both as a **CLI** and as a **REST API** (`dcx api`).

## Why dcx?

- 🧠 **AI authoring that's safe to ship.** Forced tool-calling, `temperature=0`, and strict server-side validation against the ODCS schema — the model can only produce spec-valid output, never free-form guesses.
- 🏷️ **A tag *manager*, not a tag guesser.** You define a controlled [tag catalog](#the-tag-catalog-input-for-auto-tagging) (names, allowed values, examples); the LLM classifies columns into *your* vocabulary, with optional defaults.
- ✅ **Executable, portable data quality.** Quality rules prefer ODCS `library` metrics (portable, and mappable to platform-native checks) and fall back to portable `sql` checks — across all seven ODCS dimensions.
- 🔌 **Any LLM provider.** Powered by [litellm](https://github.com/BerriAI/litellm) — Anthropic, OpenAI, Azure, Bedrock, Gemini, Ollama, … behind one `--model` flag.
- 🧩 **Pluggable platforms, no fork.** dcx registers into datacontract-cli's importer/exporter factories and extends its Typer app — so you keep all 30+ upstream importers/exporters and `lint` / `test` / `changelog`, and gain the AI + platform layer on top.
- 🔐 **Auth that makes sense per surface.** Live platform operations over the API use **caller-supplied OAuth** (no ambient server credentials); secrets are never CLI flags.

## Install

> Requires Python 3.10–3.12.

```bash
git clone https://github.com/MickaelBZH/data-contract-x.git
cd data-contract-x
pip install -e .
```

Once published to PyPI: `pip install datacontract-x` (the import package and CLI stay `dcx`).

This pulls in `datacontract-cli`, `litellm`, FastAPI, and the platform connectors automatically.

## Quickstart

The full loop — import a live schema, enrich it with an LLM, sync it back. Snowflake here is the example platform.

```bash
# 1. Import an existing schema into a contract (real columns, PKs, comments, tags)
dcx import snowflake \
  --database MY_DB --schema LOAD \
  --authenticator externalbrowser \
  --output contract.yaml

# 2. Enrich with an LLM: descriptions + constraints + tags + data-quality tests
export ANTHROPIC_API_KEY=...           # or OPENAI_API_KEY / AZURE_API_KEY / ...
dcx enrich all contract.yaml \
  --catalog tags_catalog.yaml \
  --output contract.enriched.yaml

# 3. Preview exactly what will run — no connection needed
dcx apply snowflake contract.enriched.yaml --include-quality --dry-run

# 4. Apply it: creates the table if missing, governs it (comments + tags + DQ) if it exists
dcx apply snowflake contract.enriched.yaml --include-quality
```

You can also generate the SQL as a file instead of applying it:

```bash
dcx export snowflake-full contract.enriched.yaml --include-quality --output setup.sql
```

## Concepts

### Contracts (ODCS)

Every dcx command reads and writes an [Open Data Contract Standard](https://bitol.io/) v3.1 document — the portable source of truth for a dataset's schema, semantics, governance tags, and quality rules.

### AI enrichment

`enrich` uses an LLM (via litellm) to fill in a contract. Each sub-command is independent and idempotent (existing values are preserved unless you pass `--overwrite`):

| Command | Adds |
|---|---|
| `dcx enrich columns` | Business descriptions, `logicalTypeOptions` constraints, `required` / `unique` flags |
| `dcx enrich tags` | Governance tags, classified against **your** [tag catalog](#the-tag-catalog-input-for-auto-tagging) |
| `dcx enrich quality` | An executable data-quality suite across all ODCS dimensions |
| `dcx enrich all` | columns → tags → quality, in that order so each stage grounds the next |

The provider API key is read from the environment (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, …) — there is no `--api-key` flag by design. Point `--model` at any litellm-supported model and `--base-url` at a proxy / Azure / Ollama endpoint.

### The tag catalog (input for auto-tagging)

`dcx enrich tags` does **controlled-vocabulary** tagging: instead of letting the model invent tags, you give it a catalog of allowed tag names and values, and it classifies each column into that vocabulary. The catalog is a small YAML (or JSON) file — this is the only extra input auto-tagging needs.

```yaml
# tags_catalog.yaml
tags:
  - name: DATA_CLASSIFICATION          # the tag name (becomes the platform TAG name)
    description: >                      # tells the model what this tag is for
      Data sensitivity level. Assign exactly one — the highest level that applies.
    multiple: false                    # false = at most one value per column; true = many
    values:
      - value: PUBLIC                   # an allowed value (the model may only pick from these)
        description: Non-sensitive data that can be shared freely.
        examples: [country_code, currency, language, product_category]   # guide classification
      - value: INTERNAL
        description: Internal business data, not for public release. The default.
        default: true                  # assigned when the model picks nothing else for this tag
        examples: [order_id, status, created_at, loyalty_points]
      - value: CONFIDENTIAL
        description: Personal data or sensitive business data; need-to-know access.
        examples: [full_name, email, phone, home_address, date_of_birth]
      - value: RESTRICTED
        description: Highly sensitive data under legal/regulatory controls (financial, health, credentials, IDs).
        examples: [national_id, passport_number, iban, credit_card_number, health_status]

  - name: DATA_DOMAIN                   # you can define several tags
    description: The business domain that owns the column.
    multiple: false
    values:
      - value: CUSTOMER
        examples: [customer_id, email, loyalty_points]
      - value: FINANCE
        examples: [amount, currency, invoice_id, iban]
```

**Field reference**

| Field | Meaning |
|---|---|
| `name` | Tag name. Required. Becomes the tag key everywhere downstream. |
| `description` | What the tag means — given to the model as classification guidance. |
| `multiple` | `false` (default): at most one value per column. `true`: a column may carry several values of this tag. |
| `values[].value` | An allowed value. **The model may only assign values listed here** — anything else is dropped. |
| `values[].description` | What the value means — strongly improves classification accuracy. |
| `values[].examples` | Example column names/meanings that fit this value — the model's strongest signal. |
| `values[].default` | If `true`, assigned automatically to columns the model leaves unclassified for this tag. At most one default per tag. |

**How tags flow.** Assigned tags are written on each column as `NAME=VALUE` (e.g. `DATA_CLASSIFICATION=CONFIDENTIAL`) — the exact convention `dcx export snowflake-full` and `dcx apply snowflake` consume. Use `--tag-namespace DB.SCHEMA` on export/apply to qualify the tag references for your platform's tag governance.

```bash
dcx enrich tags contract.yaml --catalog tags_catalog.yaml --output contract.tagged.yaml
```

A worked catalog and example contracts live in [`examples/`](examples/) — e.g. [`examples/tags_catalog.yaml`](examples/tags_catalog.yaml).

### Apply

`dcx apply snowflake` is built for the common case where **you don't know whether the table exists yet**. The default `--ddl-mode auto`:

- **creates** the table if it's missing (`CREATE TABLE IF NOT EXISTS`), and
- **governs** it if it exists — sets column/table comments, tags, and (with `--include-quality`) data-quality metrics — without ever failing on an existing table.

When the table already exists, dcx also **compares its live schema to the contract** and reports any drift (columns missing on either side, type mismatches) as warnings — or, with `--strict`, an error that aborts before any change. The check uses `DESCRIBE TABLE`, so it needs no active warehouse.

| Flag | Effect |
|---|---|
| `--ddl-mode auto` *(default)* | Create the table if missing, else govern the existing one |
| `--ddl-mode always` | Emit `CREATE TABLE` (errors if it already exists) |
| `--ddl-mode never` | Govern existing tables only — never `CREATE TABLE` |
| `--strict` | Fail instead of warn when an existing table's schema differs from the contract |
| `--structured-types` | Render nested columns as typed `OBJECT(field type, …)` / `ARRAY(type)` instead of bare `OBJECT`/`ARRAY` |
| `--include-quality` | Also emit data-quality metrics (Snowflake DMFs — an Enterprise feature) |
| `--create-tags` | Also emit `CREATE TAG IF NOT EXISTS` for each tag used |
| `--tag-namespace DB.SCHEMA` | Qualify tag references |
| `--dry-run` | Print the SQL without connecting or executing |

Secrets come from the environment (`SNOWFLAKE_PASSWORD`, `SNOWFLAKE_PRIVATE_KEY_PATH`, `SNOWFLAKE_TOKEN`) or `~/.snowflake/config.toml` — the CLI never takes a `--password` flag.

## Platform support

| Platform | Import | Export | Apply |
|---|---|---|---|
| **Snowflake** | ✅ live schema — columns, PKs, comments, tags | ✅ `snowflake-full` — DDL + tags + data-metric functions | ✅ create-or-govern — comments, tags, DQ, schema-drift check |
| **Kafka** / Schema Registry | ✅ topic value schema | — | — |
| **Files** (sql, avro, json, jsonschema, dbt, …) | ✅ 30+ formats via datacontract-cli | ✅ html, jsonschema, dbt, … via datacontract-cli | — |

Adding a platform is a self-contained importer / exporter / apply module (see `dcx/importers/`, `dcx/exporters/`, `dcx/apply/`). Snowflake is simply the first one wired end-to-end.

## REST API

```bash
dcx api --port 4242          # Swagger UI at http://127.0.0.1:4242/docs
```

Every CLI command is mirrored to an endpoint, with request **and** response schemas in the OpenAPI spec.

| Endpoint | Purpose | Auth |
|---|---|---|
| `POST /import/snowflake` | Live import from a Snowflake schema | Caller `Authorization: Bearer <oauth-token>` |
| `POST /enrich/columns` · `/tags` · `/quality` · `/all` | AI enrichment (tags/all take an inline catalog) | Server LLM key (env) |
| `POST /apply/snowflake` | Create-or-govern live Snowflake (`dry_run`, `ddl_mode`, `strict`, …) | Caller `Authorization: Bearer <oauth-token>` |
| `POST /target/{type}` · `/export/{format}` · `/import/{format}` | Deterministic target / export / file-import operations | — |

```bash
curl -X POST http://localhost:4242/apply/snowflake \
  -H "Authorization: Bearer $SNOWFLAKE_OAUTH_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{ "contract": { ... }, "include_quality": true }'
```

### Auth & security model

- **Live platform operations over the API act *as the caller*** — the OAuth bearer token comes from the `Authorization` header, so the server never uses ambient credentials for someone else's data.
- **Enrichment uses the server's LLM key** (read from the environment by litellm) — the LLM is dcx's own resource. Put service-level auth/quota in front of `/enrich/*` before exposing it publicly.
- **The CLI never takes secrets as flags** — platform secrets come from env vars or the platform's own config; LLM keys from the provider's standard env var.

## Command reference

| Command | What it does |
|---|---|
| `dcx import snowflake` | Build a contract from a live Snowflake schema (columns, PKs, comments, tags) |
| `dcx import kafka` | Build a contract from a Kafka topic's schema (Confluent Schema Registry) |
| `dcx import <format>` | All upstream file importers (sql, avro, json, jsonschema, dbt, …) |
| `dcx target <type>` | Bind a contract to a platform: set the server block + resolve `physicalType` |
| `dcx enrich columns / tags / quality / all` | LLM enrichment (see [AI enrichment](#ai-enrichment)) |
| `dcx export snowflake-full` | DDL + tags + data-metric functions as one SQL script (`--structured-types` for typed nesting) |
| `dcx export <format>` | All upstream exporters |
| `dcx apply snowflake` | Create-or-govern a live Snowflake table (see [Apply](#apply)) |
| `dcx api` | Serve every command as a REST endpoint |
| `dcx info` | Show dcx + datacontract-cli versions |

Run `dcx <command> --help` for full options.

## Inherited from datacontract-cli

dcx extends datacontract-cli rather than forking it, so **every upstream command works unchanged** — `dcx <command>` behaves exactly like `datacontract <command>`. Run `dcx <command> --help` for the full option list, or see the [datacontract-cli docs](https://github.com/datacontract/datacontract-cli).

| Command | Purpose | Key options |
|---|---|---|
| `dcx init [location]` | Create an empty data contract | `--template <url>`, `--overwrite` |
| `dcx lint [location]` | Validate a contract against the ODCS schema | `--json-schema <url>`, `--output <path>`, `--output-format json\|junit` |
| `dcx test [location]` | Run schema + data-quality tests against a configured server | `--server <name>`, `--schema-name <name>`, `--output-format`, `--publish`, `--logs` |
| `dcx ci [locations…]` | `test` for CI/CD — emits GitHub Actions annotations + a step summary | `--server`, `--json-schema` |
| `dcx changelog <old> <new>` | Semantic changelog between two contract versions | `--inline-references` |
| `dcx export <format> [location]` | Convert a contract to a target format | `--output <path>`, `--server`, `--schema-name`, `--dialect` |
| `dcx import <format>` | Build a contract from a source | `--source <path>`, `--dialect`, `--owner`, `--id`, `--output` |
| `dcx catalog` | Render an HTML catalog of many contracts | `--files <glob>`, `--output <dir>` |
| `dcx publish [location]` | Publish a contract to Entropy Data | `--ssl-verification` |
| `dcx dbt sync` | Sync contracts into a dbt project | (see `--help`) |

**`export` formats** — `sql`, `sql-query`, `dbt-models`, `dbt-sources`, `dbt-staging-sql`, `avro`, `avro-idl`, `jsonschema`, `pydantic-model`, `protobuf`, `odcs`, `rdf`, `html`, `markdown`, `mermaid`, `bigquery`, `dbml`, `go`, `spark`, `sqlalchemy`, `iceberg`, `sodacl`, `great-expectations`, `data-caterer`, `dcs`, `dqx`, `excel`, `custom` — plus dcx's **`snowflake-full`**.

**`import` formats** — `sql`, `avro`, `dbml`, `glue`, `bigquery`, `unity`, `jsonschema`, `json`, `odcs`, `parquet`, `csv`, `protobuf`, `spark`, `iceberg`, `excel`, `dbt` — plus dcx's live **`snowflake`** and **`kafka`**.

## How it fits with datacontract-cli

dcx is a **separate package that depends on datacontract-cli as a library** — no fork. It:

- registers new importers (`snowflake`, `kafka`) and the `snowflake-full` exporter into the upstream factories,
- adds `target`, `enrich`, `apply` sub-apps and live-import commands to the upstream Typer app,
- mirrors every command to FastAPI for `dcx api`.

So you keep all of upstream's importers, exporters, `lint`, `test`, and `changelog`, and gain the AI + platform layer on top.

## Development

```bash
pip install -e ".[dev]"
pytest          # 211 tests
ruff check dcx  # lint
```

Tests never hit live services or real LLMs — platform connections, the Schema Registry, and every LLM call are mocked, so the suite stays fast and offline.

## Contributing

Issues and PRs welcome. Please run `pytest` and `ruff check dcx` before opening a PR, and add tests for new behavior.

## License

[MIT](LICENSE) © dcx contributors.

<p align="center"><sub>Built on <a href="https://github.com/datacontract/datacontract-cli">datacontract-cli</a> · <a href="https://bitol.io/">Open Data Contract Standard</a> · <a href="https://github.com/BerriAI/litellm">litellm</a></sub></p>
