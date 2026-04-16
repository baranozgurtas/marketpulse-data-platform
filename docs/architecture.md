# Architecture Decisions

## Why This Stack?

| Decision | Rationale |
|---|---|
| **Airflow** for orchestration | Industry standard, DAG-based dependency management, built-in retry/backfill |
| **Spark** for processing | Handles schema normalization, dedup, and feature computation at scale |
| **Snowflake** for warehouse | Separates storage/compute, native SQL analytics, easy mart creation |
| **Medallion architecture** | RAW → STAGED → CURATED ensures reprocessability and clear data lineage |

## Data Flow

```
CoinGecko API ──► Airflow DAG ──► Snowflake RAW
                                       │
Alternative.me ──► Airflow DAG ──► Snowflake RAW
                                       │
                                  Spark Jobs
                                       │
                                  Snowflake STAGED
                                       │
                                  SQL / Views
                                       │
                                  Snowflake CURATED
                                       │
                                  Streamlit Dashboard
```

## Incremental Design

- **Ingestion**: Airflow DAGs use execution_date windowing. Each run fetches only the target date range.
- **Dedup-on-load**: RAW layer is append-only. Spark dedup step ensures STAGED has exactly one row per (asset_id, date).
- **Idempotency**: Re-running any DAG for the same date produces identical results — no side effects.

## Trade-offs

| Choice | Trade-off |
|---|---|
| Overwrite mode in Spark → STAGED | Simpler than MERGE for MVP; full refresh is fast at this scale. Production would use incremental MERGE. |
| CoinGecko free tier | Rate-limited (~10 req/min). Acceptable for daily batch; would need paid tier for real-time. |
| Single Spark worker | Sufficient for demo scale. Docker Compose scales horizontally by adding workers. |
| Fear & Greed as only sentiment source | Simplifies MVP. Production would add social sentiment, news NLP, funding rates. |

## Scaling Considerations

- **More assets**: Add to TRACKED_ASSETS config. Pipeline scales linearly.
- **Higher frequency**: Switch from daily to hourly DAGs + adjust Spark windowing.
- **More data sources**: Add new DAGs following the same pattern. Raw tables per source.
- **Larger volume**: Scale Spark workers in Docker Compose or move to EMR/Dataproc.
