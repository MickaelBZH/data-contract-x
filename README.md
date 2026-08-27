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

The pipeline:

```
import  ──→  enrich  ──┬──→  apply     push governance back to the platform
                       └──→  export    to SQL / docs / schemas

  import   a live schema into an ODCS contract
  enrich   columns · tags · quality
```

Everything is available both as a **CLI** and as a **REST API** (`dcx api`).

It's **platform-extensible by design** — each platform is a small importer / exporter / apply module that plugs into datacontract-cli's factories. **Snowflake is the first end-to-end platform** (import → enrich → apply), with Kafka import today and more platforms built to slot in the same way.

## Why dcx?

- 🧠 **AI authoring that's safe to ship.** Forced tool-calling, `temperature=0`, and strict server-side validation against the ODCS schema — the model can only produce spec-valid output, never free-form guesses.
- 🏷️ **A tag *manager*, not a tag guesser.** You define a controlled [tag catalog](#the-tag-catalog) (names, allowed values, examples); the LLM classifies columns into *your* vocabulary, with optional defaults.
- ✅ **Executable, portable data quality.** Quality rules prefer ODCS `library` metrics (portable, mappable to platform-native checks) and fall back to portable `sql` checks — across all seven ODCS dimensions.
- 🔌 **Any LLM provider.** Powered by [litellm](https://github.com/BerriAI/litellm) — Anthropic, OpenAI, Azure, Bedrock, Gemini, Ollama, … behind one `--model` flag.
- 🧩 **Pluggable platforms, no fork.** You keep all 30+ upstream importers/exporters and `lint` / `test` / `changelog`, and gain the AI + platform layer on top.
- 🔐 **Auth that makes sense per surface.** Live platform operations over the API use **caller-supplied credentials** — OAuth, key-pair, or password, never the server's; on the CLI, secrets are never flags.

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
| `dcx import snowflake` | A live Snowflake schema — tables **and views** (columns, primary keys, comments, tags; `physicalType` records the asset type, and a view's SELECT body is captured as a `viewDefinition`). `--quality` additionally reads attached data metric functions back into `quality` / `slaProperties` |
| `dcx import kafka` | A Kafka topic's value schema (Confluent Schema Registry) |
| `dcx import <format>` | A file/document — `sql`, `avro`, `dbml`, `glue`, `bigquery`, `unity`, `jsonschema`, `json`, `odcs`, `parquet`, `csv`, `protobuf`, `spark`, `iceberg`, `excel`, `dbt` |

```bash
dcx import snowflake --database MY_DB --schema LOAD --authenticator externalbrowser --output contract.yaml
dcx import snowflake --database MY_DB --schema LOAD --quality --output contract.yaml   # + attached DMFs
dcx import kafka --schema-registry https://sr:8081 --topic orders --output contract.yaml
dcx import sql --source schema.sql --dialect snowflake --output contract.yaml
```

**API**
- `POST /import/snowflake` — live import, authenticated by the caller's own credentials: an `auth` block (`oauth` · `key_pair` · `password`) or an `Authorization: Bearer` token. See [Connecting to Snowflake](#connecting-to-snowflake).
- `POST /import/{format}` — file-based importers; send the document inline as `source_content`.
- *(Kafka import is CLI-only.)*

### `enrich` — AI authoring with an LLM

| Sub-command | Adds |
|---|---|
| `dcx enrich columns` | Business descriptions, `logicalTypeOptions` constraints, `required` / `unique` flags |
| `dcx enrich tags` | Governance tags, classified against your [tag catalog](#the-tag-catalog) |
| `dcx enrich quality` | An executable data-quality suite across all ODCS dimensions |
| `dcx enrich all` | columns → tags → quality, in that order so each stage grounds the next |

Each sub-command is independent and **idempotent** — existing values are preserved unless you pass `--overwrite`.

| Option | Effect |
|---|---|
| `--model` | any litellm model (`claude-opus-4-8`, `gpt-4o`, `ollama/llama3`, …) |
| `--base-url` | a proxy / Azure / Ollama endpoint |
| `--overwrite` | replace existing values instead of preserving them |

The provider key is read from the environment — there is no `--api-key` flag.

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

#### `snowflake-full`

Emits the exact script [`apply --dry-run`](#apply--push-governance-to-a-live-platform) would, and shares its SQL-generation knobs:

`--ddl-mode` · `--structured-types` · `--comments` · `--include-tags` · `--include-quality` · `--create-tags` · `--tag-namespace` · `--tag-namespace-filter`

See the `apply` option table below for what each does. Only `--strict` has no export equivalent — drift detection needs a live connection.

#### `dbt`

Unifies upstream's `dbt-models` / `dbt-sources` / `dbt-staging-sql` under one command via `--kind` (those upstream commands remain available, unchanged), and maps ODCS governance the idiomatic dbt way:

| ODCS | → dbt | Why |
|---|---|---|
| `NAME=VALUE` tags | `config.meta` | key/value metadata for docs + catalogs |
| `classification`, `businessName`, `criticalDataElement` | `config.meta` | same |
| bare tags | `config.tags` | dbt selection labels |
| schema-level tags | model `config` | upstream's models exporter drops these |

| Option | Effect |
|---|---|
| `--kind models\|sources\|staging` | which artifact to emit (default `models`) |
| `--meta-key-style full\|sanitized\|short` | how a qualified Snowflake tag `DB.SCHEMA.NAME` appears in the meta key: `db.schema.name` · `db_schema_name` · `name` |
| `--tag-namespace-filter DB.SCHEMA` | repeatable — emit only tags from these namespaces |

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

With the default `--ddl-mode auto` you don't need to know whether the table exists:

- **missing** → created with `CREATE TABLE IF NOT EXISTS`
- **existing** → governed: column/table comments, tags, and (with `--include-quality`) data-quality metrics

For existing tables dcx also compares the live schema to the contract and reports **drift** as warnings — or, with `--strict`, an error that aborts before any change. The check uses `DESCRIBE TABLE`, so it needs no active warehouse.

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

#### Views

Objects with `physicalType: view` are governed as views — tags, comments and DQ use `ALTER VIEW` / `COMMENT ON VIEW`. This holds for both `apply snowflake` and `export snowflake-full`.

Column comments are the catch. Snowflake persists them **only** inside the `CREATE VIEW` column list — there is no `ALTER` path (Snowsight uses the same trick). So dcx has to recreate the view, which needs the `viewDefinition` captured on `import`:

| `--ddl-mode` | With a `viewDefinition` | Without one |
|---|---|---|
| `always` | `CREATE OR REPLACE VIEW` — **column comments updated** | view comment + column tags only |
| `auto` (default) | `CREATE VIEW IF NOT EXISTS` — column comments land on first creation only | view comment + column tags only |
| `never` | view comment + column tags only | view comment + column tags only |

> **To update an existing view's column comments, use `--ddl-mode always`.** Every other combination leaves them as they are, and dcx notes each skip.

Materialized and external tables are imported with their real `physicalType`, but are currently governed as tables.

**API**
- `POST /apply/snowflake` — authenticated by the caller's own credentials (see [Connecting to Snowflake](#connecting-to-snowflake); `dry_run` needs none). Supports `dry_run`, `ddl_mode`, `strict`, `structured_types`, `tag_namespace_filter`, … (all under `options`) and returns the executed SQL plus any drift `warnings`.

### `target` — bind a contract to a platform

`dcx target <type>` does two things: sets the contract's **server block**, and resolves each column's **`physicalType`** for that platform.

~30 types — `snowflake`, `bigquery`, `databricks`, `postgres`, `redshift`, `mysql`, `sqlserver`, `oracle`, `s3`, `kafka`, `trino`, `athena`, `glue`, `duckdb`, `local`, …

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

## Connecting to Snowflake

Two rules cover everything below:

1. **Credentials differ per surface.** The CLI takes secrets from the environment or your Snowflake connection profile — never from a flag. The API takes them from the request, so the server acts as the caller rather than with one shared identity.
2. **Where the objects land never depends on the credentials.** Every generated statement is qualified `DATABASE.SCHEMA.OBJECT` from the contract's **server block**. A connection profile, an env var, or an OAuth token decides *who you are*, never *what you touch*.

To apply the same contract to a different database, name a different server block — `--server dev` on the CLI, `"server_name": "dev"` in the API — or edit the contract. There is deliberately no `--database` / `--schema` on `apply`: those could not retarget anything, and only pointed the drift check at a different database than the one being written.

### CLI

Non-secret connection context resolves **CLI flag → env var → contract server block**:

| Flag | Env var | Also in the contract |
|---|---|---|
| `--account` | `SNOWFLAKE_ACCOUNT` | ✅ |
| `--user` | `SNOWFLAKE_USER` | — |
| `--role` | `SNOWFLAKE_ROLE` | — |
| `--secondary-roles` | `SNOWFLAKE_SECONDARY_ROLES` | — |
| `--warehouse` | `SNOWFLAKE_WAREHOUSE` | ✅ |
| `--authenticator` | `SNOWFLAKE_AUTHENTICATOR` | — |

**Secrets are environment-only — there is no `--password` flag and there never will be** (shell history, `ps aux`, CI logs):

| Env var | Used for |
|---|---|
| `SNOWFLAKE_PASSWORD` | password auth |
| `SNOWFLAKE_PRIVATE_KEY_PATH` | key-pair — `--authenticator snowflake_jwt` |
| `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` | encrypted private key |
| `SNOWFLAKE_TOKEN` | OAuth — `--authenticator oauth` |

`--authenticator` selects the method: `snowflake` (password, the default), `externalbrowser` (SSO), `oauth`, `snowflake_jwt` (key-pair). The connector auto-detects when you omit it.

`--secondary-roles` accepts `ALL` or `NONE` alone, or one or more comma-separated
Snowflake role names (for example, `DATA_READER,DATA_STEWARD`). It resolves as **CLI
flag → `SNOWFLAKE_SECONDARY_ROLES` → unset**. `ALL` and `NONE` are case-insensitive;
named roles must use valid Snowflake identifier syntax. A role name with spaces must
preserve its Snowflake identifier quotes in the shell, for example
`--secondary-roles '"Finance Reader"'`. When set, dcx executes `USE SECONDARY ROLES ...`
immediately after connecting, before metadata reads, drift checks, or DDL. When omitted,
dcx does not execute a `USE SECONDARY ROLES` statement. This is session configuration
only: it is not stored in the Data Contract server block, and dcx never selects,
switches, or retries roles. Snowflake determines whether named roles are granted to the
user or permitted by a session policy. This setting applies to dcx's Snowflake `import`
and `apply` commands; upstream `dcx test` uses datacontract-cli's connection path and
does not honor `SNOWFLAKE_SECONDARY_ROLES`.

```bash
export SNOWFLAKE_ACCOUNT=xy12345.eu-central-1 SNOWFLAKE_USER=me SNOWFLAKE_PASSWORD=...
dcx import snowflake --database MY_DB --schema LOAD --output contract.yaml

dcx apply snowflake contract.yaml --authenticator externalbrowser --role TRANSFORMER --secondary-roles NONE
```

### Connection profiles (`config.toml`)

A ready-to-edit template with all four methods lives in [`examples/snowflake_config.example.toml`](examples/snowflake_config.example.toml).

`--connection-name` uses a named profile from **the connector's own config file** — dcx defines no config format of its own and never parses these files; `snowflake-connector-python` resolves them:

```toml
# config.toml — see "Where the file lives" below
[connections.dev]
account = "xy12345.eu-central-1"
user = "SVC_DCX"
authenticator = "SNOWFLAKE_JWT"
private_key_file = "/home/me/.snowflake/rsa_key.p8"   # absolute: `~` is NOT expanded
role = "TRANSFORMER"
warehouse = "DEV_WH"
```

```bash
dcx import snowflake --connection-name dev --database MY_DB --schema LOAD
dcx apply snowflake contract.yaml --connection-name dev --server dev
```

The profile supplies the whole connection; only `--user`, `--role`, `--warehouse`, `--account` and `--authenticator` layer on top of it — plus `--database` / `--schema` on `import`, which name what to read. Env vars are not consulted at all on this path.

Profiles may equally live in `connections.toml` alongside it (same tables, without the `connections.` prefix) — the connector reads both.

#### Where the file lives

The connector resolves the directory, in this order:

1. **`$SNOWFLAKE_HOME`** if that directory exists — defaults to `~/.snowflake/`.
2. Otherwise the platform config dir: `~/.config/snowflake/` on Linux, `~/Library/Application Support/snowflake/` on macOS, `%LOCALAPPDATA%\snowflake\` on Windows.

Note the first rule tests for *existence*: with no `~/.snowflake/` directory, `~/.config/snowflake/config.toml` is the file in play. To check which one your install uses:

```bash
python -c "from snowflake.connector.constants import CONFIG_FILE; print(CONFIG_FILE)"
```

**If you set nothing at all, your default profile is used.** When no flag, no `SNOWFLAKE_*` variable and no contract server block identify a connection, dcx falls back to the profile named by `default_connection_name` — the same one `connect()` uses with no arguments — instead of erroring:

```toml
# config.toml
default_connection_name = "dev"
```

```bash
dcx apply snowflake contract.yaml        # no credentials anywhere: uses [connections.dev]
```

The contract's `account` is still layered on top, so a default profile can never silently move an apply to another account. If no default profile is configured, you get the usual `Cannot determine Snowflake account/user` error.

> Snowflake requires the file to be private: `chmod 0600` it, or the connector warns `Bad owner or permissions` on every connect.

> **`private_key_file` must be an absolute path.** The connector opens it with a bare `open()` and does not expand `~`, so a tilde path fails at connect time with `No such file or directory: '~/...'`.

> SnowSQL's `~/.snowsql/config` is a different file in a different format and is **not** read. If your profiles live there, port them to `config.toml` (`snow connection add`) or use the env vars above.

**A profile authenticates; the contract targets.** A `dev` profile applied to a contract whose server block names `PROD_DB` will authenticate as dev and still write to `PROD_DB`. Pair a profile with the matching `--server` block.

### API

The live endpoints (`POST /import/snowflake`, `POST /apply/snowflake`) take an `auth` object in the request body — the server holds no credentials of its own:

| `auth.type` | Fields |
|---|---|
| `oauth` | `token` |
| `key_pair` | `user`, `private_key`, `private_key_passphrase?` |
| `password` | `user`, `password` |
| `config` | `connection_name` (omit for the server's default profile) — **off by default**, see below |

```jsonc
POST /import/snowflake
{
  "account": "xy12345.eu-central-1", "database": "MY_DB", "schema": "LOAD",
  "secondary_roles": "NONE",
  "auth": {
    "type": "key_pair",
    "user": "SVC_DCX",
    "private_key": "-----BEGIN ENCRYPTED PRIVATE KEY-----\n...",
    "private_key_passphrase": "..."
  }
}
```

With `config` the server's profile carries the account, user and credentials, so the body needs only what to read:

```jsonc
POST /import/snowflake
{
  "database": "MY_DB", "schema": "LOAD",
  "auth": {"type": "config", "connection_name": "dev"}
}
```

Omit `connection_name` to use the server's `default_connection_name`; if it has none configured, that is a 400.

`private_key` is PEM text or base64-encoded PKCS#8 DER. There is no `private_key_file`: a path would make the server read *its own* filesystem on a caller's behalf.

`Authorization: Bearer <token>` is shorthand for `{"type": "oauth"}` and still works on its own; when both are sent, the body wins. `dry_run` on `/apply/snowflake` needs no credentials at all.

Both endpoints accept optional `secondary_roles`: use it at the top level for
`/import/snowflake`, or in the `options` object for `/apply/snowflake`. Values may be
`"ALL"`, `"NONE"`, a role name, or a comma-separated list of role names. API requests
use only this explicit request value; they never inherit
`SNOWFLAKE_SECONDARY_ROLES` from the API host.

Errors: **401** no credentials · **400** unusable key material · **403** method disabled on this server · **502** Snowflake refused.

#### Server-side profiles — `dcx api --allow-local-credentials`

`auth.type: config` is the one method that reads the **API host's** own connection config instead of the caller's credentials, so it is disabled unless you start the server with it:

```bash
dcx api --allow-local-credentials                              # only when the server is yours alone
dcx api --allow-local-credentials --snowflake-config /etc/dcx  # profiles from a specific location
```

`--snowflake-config` takes the directory holding `config.toml` / `connections.toml` (or the file itself) and points the connector at it via `SNOWFLAKE_HOME`, so a service account's profiles need not live in the server user's home. It is validated at startup — a missing path, an unreadable filename, or a directory with no config in it fails immediately rather than silently falling back to the default location. It requires `--allow-local-credentials`, since without that the server never reads a Snowflake config at all.

Whenever server-side profiles are enabled, startup reports which config is in play and what it contains (names only, never values):

```
Snowflake config: /etc/dcx/config.toml (4 profiles: dev, sso, pw, oauth; default: dev)
Server-side profiles enabled: any caller may authenticate as this host via `auth.type: config`.
```

A config that cannot be read, or that defines no profiles, is reported as a warning at startup rather than surfacing later as a failed request.

`dcx api` has no authentication of its own. On a shared instance a server-side profile would let anyone who can reach the port connect as whoever runs the server — so it stays off by default, and requests using it get a `403`. On a personal localhost server it is the safest option available, since no secret crosses the wire. Only a profile *name* is accepted, never a path.

## The tag catalog

`dcx enrich tags` does **controlled-vocabulary** tagging. Instead of letting the model invent tags, you give it a catalog of allowed names and values, and it classifies each column into that vocabulary.

The catalog is a small YAML (or JSON) file — the only extra input auto-tagging needs.

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

- **Live platform operations** (`/import/snowflake`, `/apply/snowflake`) act *as the caller* — credentials ride on the request (an `auth` block or a bearer token), so the server never uses ambient credentials for someone else's data. The one exception, server-side connection profiles, is off unless you start the server with `--allow-local-credentials`. Details: [Connecting to Snowflake](#connecting-to-snowflake).
- **Enrichment** (`/enrich/*`) uses the **server's** LLM key (from the environment). Put service-level auth/quota in front of it before exposing it publicly.
- **The CLI never takes secrets as flags** — platform secrets come from env vars or the connector's own `config.toml`; LLM keys from the provider's standard env var. (Over HTTP a caller *does* send credentials in the request body — that is the point: they are the caller's, not the server's. Terminate TLS in front of it.)

## How it fits with datacontract-cli

dcx is a **separate package that depends on datacontract-cli as a library** — no fork. It plugs into upstream's own extension points:

| dcx adds | Where it plugs in |
|---|---|
| importers `snowflake`, `kafka` | upstream's `importer_factory` |
| exporter `snowflake-full` | upstream's `exporter_factory` |
| `target` / `enrich` / `apply` sub-apps | upstream's Typer app |
| REST routes for every command | FastAPI, via `dcx api` |

So you keep all of upstream's importers, exporters, `lint`, `test` and `changelog`, and gain the AI + platform layer on top.

## Development

```bash
pip install -e ".[dev]"
pytest          # 375 tests
ruff check dcx  # lint
```

Tests never hit live services or real LLMs — platform connections, the Schema Registry, and every LLM call are mocked, so the suite stays fast and offline. See [`RELEASING.md`](RELEASING.md) for the PyPI release process.

## Contributing

Issues and PRs welcome. Please run `pytest` and `ruff check dcx` before opening a PR, and add tests for new behavior.

## License

[MIT](LICENSE) © MickaelBZH.

<p align="center"><sub>Built on <a href="https://github.com/datacontract/datacontract-cli">datacontract-cli</a> · <a href="https://bitol.io/">Open Data Contract Standard</a> · <a href="https://github.com/BerriAI/litellm">litellm</a></sub></p>
