<p align="center">
  <img src="assets/logo.svg" alt="dcx — Data Contract eXtended" width="520">
</p>

<h3 align="center">Data Contract e<strong>X</strong>tended — AI-native, platform-extensible data contracts</h3>

<p align="center">
  Author data contracts with an LLM, sync them with your live platforms.<br>
  A lean, no-fork extension of <a href="https://github.com/datacontract/datacontract-cli">datacontract-cli</a>, built on the <a href="https://bitol.io/">Open Data Contract Standard (ODCS)</a>.
</p>

<p align="center">
  <img alt="PyPI" src="https://img.shields.io/pypi/v/datacontract-x?color=6366F1&label=pypi">
  <img alt="Python" src="https://img.shields.io/badge/python-3.10%20|%203.11%20|%203.12-3776AB?logo=python&logoColor=white">
  <img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-22c55e">
  <img alt="ODCS" src="https://img.shields.io/badge/ODCS-v3.1-0EA5E9">
  <img alt="Built on datacontract-cli" src="https://img.shields.io/badge/built%20on-datacontract--cli-6366F1">
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
- 🏷️ **A tag *manager*, not a tag guesser.** You define a controlled [tag catalog](#the-tag-catalog) (names, allowed values, examples); the LLM classifies columns into *your* vocabulary, with optional defaults.
- ✅ **Executable, portable data quality.** Quality rules prefer ODCS `library` metrics (portable, mappable to platform-native checks) and fall back to portable `sql` checks — across all seven ODCS dimensions.
- 🔌 **Any LLM provider.** Powered by [litellm](https://github.com/BerriAI/litellm) — Anthropic, OpenAI, Azure, Bedrock, Gemini, Ollama, … behind one `--model` flag.
- 🧩 **Pluggable platforms, no fork.** You keep all 30+ upstream importers/exporters and `lint` / `test` / `changelog`, and gain the AI + platform layer on top.
- 🔐 **Auth that makes sense per surface.** Live platform operations over the API use **caller-supplied OAuth**; secrets are never CLI flags.

## Install

```bash
pip install datacontract-x
```

The import package and CLI are both `dcx`:

```bash
dcx --help
dcx info
```

From source (for development):

```bash
git clone https://github.com/MickaelBZH/data-contract-x.git
cd data-contract-x
pip install -e ".[dev]"
```

> Requires Python 3.10–3.12. Installing pulls in `datacontract-cli`, `litellm`, FastAPI, and the platform connectors automatically.

## Quickstart

The full loop — import a live schema, enrich it with an LLM, sync it back. Snowflake here is the example platform.

```bash
# 1. Import an existing schema into a contract (real columns, PKs, comments, tags)
dcx import snowflake --database MY_DB --schema LOAD --authenticator externalbrowser --output contract.yaml

# 2. Enrich with an LLM: descriptions + constraints + tags + data-quality tests
export ANTHROPIC_API_KEY=...           # or OPENAI_API_KEY / AZURE_API_KEY / ...
dcx enrich all contract.yaml --catalog tags_catalog.yaml --output contract.enriched.yaml

# 3. Preview exactly what will run — no connection needed
dcx apply snowflake contract.enriched.yaml --include-quality --dry-run

# 4. Apply it: creates the table if missing, governs it (comments + tags + DQ) if it exists
dcx apply snowflake contract.enriched.yaml --include-quality
```

---

## Commands

Every command is `dcx <command>`, and most are mirrored to a REST endpoint when you run [`dcx api`](#rest-api). Each section below lists the sub-commands, a CLI example, and the matching API call. Run `dcx <command> --help` for the full option list.

### `import` — build a contract from a source

| Sub-command | Source |
|---|---|
| `dcx import snowflake` | A live Snowflake schema — tables **and views** (columns, primary keys, comments, tags; `physicalType` records the asset type) |
| `dcx import kafka` | A Kafka topic's value schema (Confluent Schema Registry) |
| `dcx import <format>` | A file/document — `sql`, `avro`, `dbml`, `glue`, `bigquery`, `unity`, `jsonschema`, `json`, `odcs`, `parquet`, `csv`, `protobuf`, `spark`, `iceberg`, `excel`, `dbt` |

```bash
dcx import snowflake --database MY_DB --schema LOAD --authenticator externalbrowser --output contract.yaml
dcx import kafka --schema-registry https://sr:8081 --topic orders --output contract.yaml
dcx import sql --source schema.sql --dialect snowflake --output contract.yaml
```

**API**
- `POST /import/snowflake` — live import, authenticated by the caller's Snowflake OAuth token (`Authorization: Bearer <token>`).
- `POST /import/{format}` — file-based importers; send the document inline as `source_content`.
- *(Kafka import is CLI-only.)*

### `enrich` — AI authoring with an LLM

| Sub-command | Adds |
|---|---|
| `dcx enrich columns` | Business descriptions, `logicalTypeOptions` constraints, `required` / `unique` flags |
| `dcx enrich tags` | Governance tags, classified against your [tag catalog](#the-tag-catalog) |
| `dcx enrich quality` | An executable data-quality suite across all ODCS dimensions |
| `dcx enrich all` | columns → tags → quality, in that order so each stage grounds the next |

Each sub-command is independent and idempotent (existing values are preserved unless you pass `--overwrite`). The provider key is read from the environment — there is no `--api-key` flag. Use `--model` for any litellm model and `--base-url` for a proxy / Azure / Ollama endpoint.

```bash
dcx enrich columns contract.yaml --output contract.enriched.yaml
dcx enrich tags    contract.yaml --catalog tags_catalog.yaml --output contract.tagged.yaml
dcx enrich quality contract.yaml --model gpt-4o --output contract.dq.yaml
dcx enrich all     contract.yaml --catalog tags_catalog.yaml --output contract.full.yaml
```

**API** (the LLM key comes from the *server's* environment)
- `POST /enrich/columns` · `POST /enrich/quality`
- `POST /enrich/tags` · `POST /enrich/all` — take the tag catalog inline in the request body.

### `export` — convert a contract to a target format

| Sub-command | Output |
|---|---|
| `dcx export snowflake-full` | A Snowflake setup script: DDL + tags + Data Metric Functions, in one file |
| `dcx export dbt` | dbt `models` / `sources` / `staging`, with ODCS governance mapped to `config.meta` / `config.tags` |
| `dcx export <format>` | Any upstream format — `sql`, `jsonschema`, `html`, `markdown`, `mermaid`, `dbt-*`, `avro`, `protobuf`, `bigquery`, `spark`, `sqlalchemy`, `iceberg`, `sodacl`, `great-expectations`, `dbml`, `pydantic-model`, `odcs`, `rdf`, `go`, `excel`, … |

`snowflake-full` shares [`apply`](#apply)'s SQL-generation knobs, so it emits the exact same script `apply --dry-run` would: `--ddl-mode auto\|always\|never` (default `auto` → `CREATE TABLE IF NOT EXISTS` + govern), `--structured-types`, `--comments`, `--include-tags`, `--include-quality`, `--create-tags`, `--tag-namespace DB.SCHEMA`, `--tag-namespace-filter DB.SCHEMA` (repeatable — emit only tags from these namespaces, e.g. to skip centrally-managed/inherited ones). (`apply`'s `--strict` drift check has no export equivalent — it needs a live connection.)

`dbt` unifies upstream's `dbt-models` / `dbt-sources` / `dbt-staging-sql` under one command via `--kind models\|sources\|staging` (default `models`), and maps ODCS governance the idiomatic dbt way: `NAME=VALUE` tags plus `classification` / `businessName` / `criticalDataElement` go to **`config.meta`** (key/value metadata for docs + catalogs), while bare tags become **`config.tags`** (dbt selection labels). Schema-level tags — dropped by the upstream models exporter — land on the model's `config`. Tags imported from Snowflake are fully qualified (`DB.SCHEMA.NAME`); `--meta-key-style full\|sanitized\|short` controls how that namespace appears in the meta key (`db.schema.name` · `db_schema_name` · `name`), and `--tag-namespace-filter DB.SCHEMA` (repeatable) restricts output to tags from the given namespaces. (The upstream `dbt-models` / `dbt-sources` / `dbt-staging-sql` commands remain available, unchanged.)

```bash
dcx export snowflake-full contract.yaml --include-quality --create-tags --output setup.sql
dcx export snowflake-full contract.yaml --ddl-mode never --output govern.sql   # alter-only
dcx export dbt contract.yaml --kind models --server production --output schema.yml
dcx export html contract.yaml --output contract.html
```

**API**
- `POST /export/{format}` — including `POST /export/snowflake-full` and `POST /export/dbt` (`{options: {kind: "models"}}`). The response media type depends on the format (JSON / YAML / text / binary).

### `apply` — push governance to a live platform

| Sub-command | Target |
|---|---|
| `dcx apply snowflake` | A live Snowflake account |

With the default `--ddl-mode auto` you don't need to know whether the table exists: **missing tables are created** (`CREATE TABLE IF NOT EXISTS`) and **existing ones are governed** — column/table comments, tags, and (with `--include-quality`) data-quality metrics. For existing tables, dcx also **compares the live schema to the contract** and reports drift as warnings — or, with `--strict`, an error that aborts before any change (the check uses `DESCRIBE TABLE`, so it needs no active warehouse).

Objects with `physicalType: view` are **governed as views** — no `CREATE` (the contract has no view definition), and comments/tags/DQ use `COMMENT ON VIEW` / `ALTER VIEW`. This holds for both `apply snowflake` and `export snowflake-full`. (Materialized/external tables are imported with their real `physicalType` but currently governed as tables.)

| Option | Effect |
|---|---|
| `--ddl-mode auto\|always\|never` | create-if-missing-then-govern (default) · always `CREATE TABLE` · govern existing only |
| `--strict` | fail instead of warn on schema drift |
| `--structured-types` | typed nested `OBJECT(...)` / `ARRAY(...)` |
| `--include-quality` · `--create-tags` · `--tag-namespace` | data-metric functions · `CREATE TAG IF NOT EXISTS` · qualify *bare* tag refs (already-namespaced `DB.SCHEMA.NAME` tags are left as-is) |
| `--tag-namespace-filter DB.SCHEMA` | repeatable — apply only tags from these namespaces (skip centrally-managed/inherited ones); un-namespaced tags are skipped |
| `--dry-run` | print the SQL without connecting |

```bash
dcx apply snowflake contract.yaml --dry-run            # preview
dcx apply snowflake contract.yaml --include-quality    # create-or-govern
```

**API**
- `POST /apply/snowflake` — authenticated by the caller's Snowflake OAuth token. Supports `dry_run`, `ddl_mode`, `strict`, `structured_types`, `tag_namespace_filter`, … (all under `options`) and returns the executed SQL plus any drift `warnings`.

### `target` — bind a contract to a platform

`dcx target <type>` sets the contract's server block and resolves each column's `physicalType` for that platform. ~30 types: `snowflake`, `bigquery`, `databricks`, `postgres`, `redshift`, `mysql`, `sqlserver`, `oracle`, `s3`, `kafka`, `trino`, `athena`, `glue`, `duckdb`, `local`, …

```bash
dcx target snowflake contract.yaml --output contract.snowflake.yaml
```

**API**
- `POST /target/{type}` — one route per supported platform type.

### From datacontract-cli

These commands work unchanged — `dcx <command>` behaves exactly like `datacontract <command>`.

| Command | Sub-commands | Purpose | API |
|---|---|---|---|
| `dcx init` | — | Create an empty data contract | — |
| `dcx lint` | — | Validate a contract against the ODCS schema | `POST /lint` |
| `dcx test` | — | Run schema + data-quality tests against a configured server | `POST /test` |
| `dcx ci` | — | `test` for CI/CD — emits GitHub Actions annotations | — |
| `dcx changelog` | — | Semantic changelog between two contract versions | `POST /changelog` |
| `dcx catalog` | — | Render an HTML catalog of many contracts | — |
| `dcx publish` | — | Publish a contract to Entropy Data | — |
| `dcx dbt` | `sync` | Sync contracts into a dbt project | — |

### `api` / `info`

```bash
dcx api --port 4242      # start the REST server (Swagger UI at /docs)
dcx info                 # show dcx + datacontract-cli versions   (API: GET /info)
```

---

## The tag catalog

`dcx enrich tags` does **controlled-vocabulary** tagging: instead of letting the model invent tags, you give it a catalog of allowed names and values, and it classifies each column into that vocabulary. The catalog is a small YAML (or JSON) file — the only extra input auto-tagging needs.

```yaml
# tags_catalog.yaml
tags:
  - name: DATA_CLASSIFICATION          # the tag name (becomes the platform TAG name)
    description: >                      # tells the model what this tag is for
      Data sensitivity level. Assign exactly one — the highest level that applies.
    multiple: false                    # false = at most one value per column; true = many
    values:
      - value: PUBLIC                   # the model may only pick from these values
        description: Non-sensitive data that can be shared freely.
        examples: [country_code, currency, language, product_category]   # guide classification
      - value: INTERNAL
        description: Internal business data, not for public release. The default.
        default: true                  # assigned when the model picks nothing else
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

| Field | Meaning |
|---|---|
| `name` | Tag name. Required. Becomes the tag key everywhere downstream. |
| `description` | What the tag means — given to the model as classification guidance. |
| `multiple` | `false` (default): at most one value per column. `true`: a column may carry several. |
| `values[].value` | An allowed value. **The model may only assign values listed here** — anything else is dropped. |
| `values[].description` | What the value means — strongly improves accuracy. |
| `values[].examples` | Example column names that fit this value — the model's strongest signal. |
| `values[].default` | If `true`, assigned to columns the model leaves unclassified for this tag. At most one per tag. |

Assigned tags are written on each column as `NAME=VALUE` (e.g. `DATA_CLASSIFICATION=CONFIDENTIAL`) — the convention `export snowflake-full` and `apply snowflake` consume. A worked catalog and example contracts live in [`examples/`](examples/).

## REST API

```bash
dcx api --port 4242      # Swagger UI at http://127.0.0.1:4242/docs
```

Every command above is mirrored to an endpoint, with request **and** response schemas in the OpenAPI spec. Auth model:

- **Live platform operations** (`/import/snowflake`, `/apply/snowflake`) act *as the caller* — the OAuth bearer token comes from the `Authorization` header, so the server never uses ambient credentials for someone else's data.
- **Enrichment** (`/enrich/*`) uses the **server's** LLM key (from the environment). Put service-level auth/quota in front of it before exposing it publicly.
- **The CLI never takes secrets as flags** — platform secrets come from env vars or the platform's own config; LLM keys from the provider's standard env var.

## How it fits with datacontract-cli

dcx is a **separate package that depends on datacontract-cli as a library** — no fork. It registers new importers (`snowflake`, `kafka`) and the `snowflake-full` exporter into the upstream factories, adds `target` / `enrich` / `apply` sub-apps and live-import commands to the upstream Typer app, and mirrors every command to FastAPI for `dcx api`. So you keep all of upstream's importers, exporters, `lint`, `test`, and `changelog`, and gain the AI + platform layer on top.

## Development

```bash
pip install -e ".[dev]"
pytest          # 211 tests
ruff check dcx  # lint
```

Tests never hit live services or real LLMs — platform connections, the Schema Registry, and every LLM call are mocked, so the suite stays fast and offline. See [`RELEASING.md`](RELEASING.md) for the PyPI release process.

## Contributing

Issues and PRs welcome. Please run `pytest` and `ruff check dcx` before opening a PR, and add tests for new behavior.

## License

[MIT](LICENSE) © MickaelBZH.

<p align="center"><sub>Built on <a href="https://github.com/datacontract/datacontract-cli">datacontract-cli</a> · <a href="https://bitol.io/">Open Data Contract Standard</a> · <a href="https://github.com/BerriAI/litellm">litellm</a></sub></p>
