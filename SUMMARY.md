# SUMMARY — dbt Integration + Kubernetes (k3s) Deployment

## Files added

| Path | Purpose |
|---|---|
| `dbt/dbt_project.yml` | dbt project config; staging → STAGED (views), marts → CURATED (tables) |
| `dbt/profiles.yml` | Snowflake profile driven entirely by `SNOWFLAKE_*` env vars — no credentials on disk |
| `dbt/macros/generate_schema_name.sql` | Makes dbt write to exact schema names (`STAGED`, `CURATED`) instead of `<target>_<suffix>` |
| `dbt/models/staging/sources.yml` | Sources over Spark-produced `STAGED.stg_prices` / `stg_sentiment` + source tests |
| `dbt/models/staging/stg_market_prices.sql` | Thin staging view over `stg_prices` |
| `dbt/models/staging/stg_market_sentiment.sql` | Thin staging view over `stg_sentiment` |
| `dbt/models/staging/schema.yml` | not_null/unique + `accepted_values` on sentiment classification |
| `dbt/models/marts/dim_asset.sql` | Migrated from `sp_refresh_dim_asset()` (MERGE → full rebuild; tiny table) |
| `dbt/models/marts/fact_market_daily.sql` | Migrated from `sp_build_fact_market_daily()` (MERGE → full rebuild) |
| `dbt/models/marts/mart_volatility.sql` | Migrated 1:1 from the `mart_volatility` view |
| `dbt/models/marts/schema.yml` | unique/not_null on PKs, `relationships` fact→dim, `accepted_values` on `volatility_regime` |
| `dbt/tests/assert_fact_market_daily_unique_key.sql` | Singular test: (asset_id, date) grain uniqueness |
| `dbt/tests/assert_prices_non_negative.sql` | Singular test: OHLCV/volume/market_cap never negative |
| `dbt/tests/assert_no_future_dates.sql` | Singular test: no future-dated records |
| `dbt/tests/assert_fear_greed_in_range.sql` | Singular test: Fear & Greed value within 0–100 |
| `dags/dag_transform_dbt.py` | New DAG: `dbt run` → `dbt test` via BashOperator, daily 07:30 UTC |
| `dags/utils/dbt_commands.py` (+`__init__.py`) | Pure, testable dbt command builder used by the DAG |
| `tests/test_dbt_commands.py` | 8 unit tests for the command builder |
| `scripts/setup_snowflake.sql` | Idempotent provisioning of warehouse/database/schemas/tables for a fresh trial account |
| `k8s/00-namespace.yaml` … `k8s/07-spark.yaml` | Plain manifests: Namespace, ConfigMap, Secret template, Postgres (+PVC), Airflow init Job, webserver (+NodePort Service), scheduler, Spark master/worker |
| `Makefile` | `cluster` / `build` / `import` / `deploy` / `status` / `teardown` / `destroy` for local k3d |
| `.env.example` | Snowflake credential template consumed by dbt and docker-compose |
| `SUMMARY.md` | This file |

## Files modified

| Path | Change |
|---|---|
| `docker/Dockerfile.airflow` | Installs dbt-core 1.7.14 + dbt-snowflake 1.7.5 in an **isolated venv** (symlinked to PATH) to avoid Airflow dependency conflicts; bakes `dags/` and `dbt/` into the image for Kubernetes |
| `docker/Dockerfile.spark` | Bakes `spark_jobs/` into the image for Kubernetes |
| `docker/docker-compose.yml` | Build context changed to repo root (needed for the COPYs above; bind mounts still override for live dev); Snowflake connection now built from `SNOWFLAKE_*` env vars; mounts `dbt/` into Airflow containers |
| `README.md` | Mermaid architecture diagram; new "dbt Transformations" and "Kubernetes Deployment (k3s)" sections; updated tech stack, project structure, testing (31 tests), design decisions |
| `.gitignore` | Added `dbt/target/`, `dbt/dbt_packages/`, `dbt/logs/`, `k8s/02-secret.yaml` |

No existing files were removed. No existing tests were modified.

## Design decisions worth defending

- **dbt owns STAGED → CURATED only.** Spark keeps RAW → STAGED. Sources point at the Spark tables; staging models are thin renamed views (`stg_market_*`) so they never collide with Spark's `stg_*` tables.
- **MERGE → full rebuild.** The stored procedures used MERGE upserts; the dbt models rebuild the tables. At ~10 assets × 365 days this is cheap, idempotent, and simpler. The model files note the switch to incremental materialization if volume grows.
- **Isolated dbt venv in the Airflow image.** dbt and Airflow pin conflicting shared libraries; a venv + symlink sidesteps that while keeping `BashOperator` calls trivially debuggable (`dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt` is exactly reproducible in a shell).
- **New DAG instead of extending an existing one.** No transform DAG existed (CURATED was built manually). `marketpulse_transform_dbt` at 07:30 UTC slots between ingestion (06:00/06:30) and quality checks (08:00).
- **Plain manifests, `imagePullPolicy: Never`, NodePort.** No Helm, no registry, no Ingress — the smallest set of Kubernetes concepts that runs the stack on local k3s/k3d.

## Pre-existing inconsistency (not changed)

`load_data.py` inserts volatility columns (`volatility_7d`, `volatility_30d`, `max_drawdown_30d`, `volatility_regime`) directly into `fact_market_daily`, but the DDL in `sql/04_curated_tables.sql` does not define those columns (they live in the `mart_volatility` view). The dbt migration follows the `sql/04` DDL as canonical; the Streamlit app reads `mart_volatility` and is unaffected. If you rely on `load_data.py`'s `build_curated()` step against DDL-created tables, it would fail today as well — after this change, prefer `dbt run` for the CURATED build.

Also note: `fact_market_daily.log_return_1d` in dbt adds a `close > 0` guard (matching `load_data.py`) that the stored procedure lacked; Spark already filters `close <= 0` upstream, so results are identical on valid data.

## Verified in this environment

- `pytest tests/` — **31/31 pass** (23 pre-existing + 8 new). Baseline before changes: 23/23.
- `dbt parse` — passes (dbt 1.11; deprecation warnings only, about the newer `arguments:` test syntax — classic syntax kept for cross-version compatibility).
- `dbt compile` — the Snowflake adapter opens a connection even for compile, so compilation was verified with a throwaway local (DuckDB) profile: all 5 models and 4 singular tests render valid SQL.
- Kubernetes YAML — all 13 objects validated against Kubernetes 1.28 OpenAPI schemas in strict mode (`kubernetes-validate`; kubeconform binary was not downloadable in this sandbox — re-run `kubeconform -strict -summary k8s/*.yaml` locally if you want a second opinion).
- `docker-compose.yml` — YAML syntax validated.

## Requires your machine to verify

- `dbt debug` / `dbt run` / `dbt test` against real Snowflake credentials (no live connection here).
- Docker image builds (no Docker daemon in this sandbox) — in particular the dbt venv layer in `Dockerfile.airflow`.
- End-to-end k3d deploy (`make cluster build import deploy`).
- Airflow DAG import of `dag_transform_dbt.py` inside the container (the helper logic is unit-tested; the DAG file itself follows the exact structure of the existing DAGs).
- Ingestion DAG runs against the live CoinGecko / Alternative.me APIs.
