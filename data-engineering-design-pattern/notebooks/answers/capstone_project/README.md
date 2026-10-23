# E-Commerce Ad Analytics Warehouse

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Apache Iceberg](https://img.shields.io/badge/Apache%20Iceberg-189BCC?style=for-the-badge&logo=apache&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Altair](https://img.shields.io/badge/Altair-4C78A8?style=for-the-badge&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)

A production-grade medallion data warehouse built on PySpark + Apache Iceberg, ingesting ad event data from Postgres and delivering performance metrics to marketing stakeholders.

---

## Stakeholders & Outcomes

Our stakeholders couldn't answer core business questions from Postgres directly — too slow, too raw, no aggregations.

| Stakeholder | Core Question | Key Metrics |
|---|---|---|
| **Marketing Agents** | Is my spend efficient, and where is the funnel leaking? | ROAS, CVR, CPA, CTR, rolling l7d/l30d/l90d CVR |

| Metric | Formula |
|---|---|
| **ROAS** | `total_revenue / total_spend` |
| **CTR** | `total_clicks / total_impressions` |
| **CVR** | `total_conversions / total_clicks` |
| **CPA** | `total_spend / total_conversions` |
| **l7d / l30d / l90d CVR** | Rolling window CVR over last 7, 30, 90 days |

---

## Results

### Ad Funnel — Where Customers Drop Off
![Ad Funnel](funnel.png)

### ROAS Over Time — Ad Spend Efficiency
![ROAS Over Time](roas_over_time.png)

---

## Architecture

A `medallion architecture` ingesting 7 Postgres ad tables into Bronze → Silver → Gold.
```mermaid
flowchart LR
    subgraph PG["Source-Postgres"]
        p1[advertiser] 
        p2[campaign]
        p3[ad_group]
        p4[ad_creative]
        p5[ad_impression]
        p6[ad_click]
        p7[ad_conversion]
    end
    subgraph BR["Bronze"]
        b1[advertiser]
        b2[campaign]
        b3[ad_group]
        b4[ad_creative]
        b5[ad_impression]
        b6[ad_click]
        b7[ad_conversion]
    end
    subgraph SL["Silver"]
        s1[dim_advertiser]
        s2[dim_campaign]
        s9[fct_ad_impressions]
        s10[fct_ad_clicks]
        s11[fct_ad_conversions]
    end
    subgraph GD["Gold"]
        g2[obt_ad]
        g4[ad_campaign_performance]
        g5[ad_funnel_summary]
    end
    p1 --> b1 --> s1
    p2 --> b2 --> s2
    p3 --> b3 --> s2
    p4 --> b4 --> s9
    p5 --> b5 --> s9
    p6 --> b6 --> s10
    p7 --> b7 --> s11
    s1 --> g2
    s2 --> g2
    s9 --> g2
    s10 --> g2
    s11 --> g2
    g2 --> g4
    g2 --> g5

    classDef bronze fill:#7c3f00,stroke:#5a2d00,color:#fff
    classDef silver fill:#c0c0c0,stroke:#a8a8a8,color:#333
    classDef gold fill:#ffd700,stroke:#daa520,color:#333
    class b1,b2,b3,b4,b5,b6,b7 bronze
    class s1,s2,s9,s10,s11 silver
    class g2,g4,g5 gold
```

**Key design principles:**
- **Separation of concerns** — raw ingestion, correctness, and aggregation are independent layers
- **Idempotency** — any window can be rerun without data corruption via `overwritePartitions()`
- **Schema evolution safety** — explicit `SELECT col1, col2` everywhere, never `SELECT *`
- **Replay without Postgres** — Bronze decouples Silver/Gold from source availability

---

## Code Pattern

Every script follows a strict 4-function pattern:
```python
extract(spark, start_time, end_time) → dict[str, DataFrame]
transform(input_dfs)                 → DataFrame
validate(output_df, spark)           → bool     
load(output_df, spark)               → None
```

**Bronze** — Full snapshot load.

**Silver dims** — `createOrReplace()` on every run. Enrichment is computed at this layer.

**Silver facts** — Incremental by `created_at` window, partitioned by `created_at`. `overwritePartitions()` makes reruns safe (**idempotent**) and handles late-arriving events.

**Gold OBT** — Wide impression-grain table joining all Silver facts and dims. Incremental load with `overwritePartitions()`.

**Gold summaries** — Aggregate from OBT. Full `createOrReplace()` on every run for maintainability.

---

## Data Quality

Validation uses Soda Core contracts following the `WAP (Write-Audit-Publish) pattern.`

Checks are defined in per-table YAML contracts under `gold/contracts/`:

| Table | Checks |
|---|---|
| `ad_campaign_performance` | `row_count > 0`, no nulls on `campaign_id` + `event_date`, `total_spend >= 0`, `roas >= 0` |
| `ad_funnel_summary` | `row_count > 0`, no nulls on `ad_group_id` + `event_date`, `total_spend >= 0`, `cvr` between 0 and 1 |