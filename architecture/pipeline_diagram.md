# Pipeline Architecture

Mermaid renders natively on GitHub and diffs as text, so the diagram reviews
like code instead of like a binary attachment. `pipeline_diagram.svg` and
`pipeline_diagram.png` are exported views of the same design, committed so the
diagram is readable without a Mermaid renderer (in a plain editor, a Word
document, or a printout).

![Pipeline architecture](pipeline_diagram.png)

## End-to-end flow

```mermaid
flowchart LR
    subgraph SRC["Source"]
        CSV["airlines_flights_data.csv<br/>300,153 rows · 12 cols<br/><i>no date column</i>"]
    end

    subgraph BRONZE["BRONZE — raw, immutable"]
        direction TB
        DRIFT{"schema drift<br/>check"}
        HASH{"file hash<br/>already seen?"}
        BZ[("bronze/flights<br/>Parquet<br/>partitioned by as_of_date")]
        MAN[("audit/ingestion_manifest")]
    end

    subgraph SILVER["SILVER — trusted"]
        direction TB
        STD["standardize<br/>trim · casing · derive"]
        VAL["validate<br/>16 accumulating rules"]
        DEDUP["dedupe on business key<br/>tiebreak: latest _ingested_at"]
        SV[("silver/flights_valid")]
        SR[("silver/flights_rejected<br/>+ rejection_reason")]
        RECON{"reconcile<br/>bronze = valid + rejected"}
    end

    subgraph DBT["dbt — modelling"]
        direction TB
        STG["staging<br/>stg_flights (view)"]
        INT["intermediate<br/>int_flight_routes<br/>int_flight_pricing_enriched<br/>(ephemeral)"]
        FCT["fct_flight_price_quote<br/>(incremental)"]
        DIMS["dim_airline · dim_route<br/>(table)"]
    end

    subgraph GOLD["GOLD — business-ready marts"]
        direction TB
        M1["mart_route_performance"]
        M2["mart_booking_leadtime_pricing"]
        M3["mart_airline_cabin_mix"]
        M4["mart_data_quality_summary"]
    end

    subgraph RS["Amazon Redshift"]
        direction TB
        SPEC["Spectrum external schema<br/>cold history"]
        HOT["gold schema<br/>DISTKEY route_id<br/>SORTKEY as_of_date"]
    end

    BI["BI / dashboards"]

    CSV --> DRIFT --> HASH --> BZ
    HASH -.->|duplicate file| SKIP["skip run — no-op"]
    BZ --> MAN
    BZ --> STD --> VAL --> DEDUP
    DEDUP --> SV
    DEDUP --> SR
    SV --> RECON
    SR --> RECON
    RECON -->|pass| STG
    RECON -.->|fail| ALERT["fail run + alert"]
    STG --> INT --> FCT
    INT --> DIMS
    FCT --> M1 & M2 & M3
    SR --> M4
    M1 & M2 & M3 & M4 --> HOT
    SV --> SPEC
    HOT --> BI
    SPEC --> BI
```

## Daily incremental run

What actually happens when tomorrow's file lands — and why a rerun is safe.

```mermaid
sequenceDiagram
    autonumber
    participant F as Daily file
    participant B as Bronze (PySpark)
    participant M as audit.ingestion_manifest
    participant S as Silver (PySpark)
    participant D as dbt
    participant R as Redshift

    F->>B: file arrives
    B->>B: assert_no_schema_drift()
    B->>M: md5(file) seen for this as_of_date?
    alt already ingested
        M-->>B: yes
        B-->>F: skip — no rows written, no double count
    else new content
        M-->>B: no
        B->>B: mint as_of_date, _batch_id, _ingested_at
        B->>B: write partition as_of_date=YYYY-MM-DD<br/>(dynamic overwrite — replaces itself only)
        B->>M: record file hash + row count
        B->>S: partition ready
        S->>S: standardize → validate → dedupe
        S->>S: write valid + rejected partitions
        S->>S: reconcile counts, fail loud on mismatch
        S->>D: Silver partition ready
        D->>D: fct_flight_price_quote (incremental)<br/>delete+insert on flight_quote_sk<br/>where as_of_date >= max(as_of_date)
        D->>D: rebuild marts, run 72 tests
        D->>R: MERGE into gold.fct_flight_price_quote
        R->>R: ANALYZE (VACUUM on schedule)
    end
```

## Layer contracts

| Layer | Writes what | Never does |
|---|---|---|
| **Bronze** | Every source row, plus metadata we control | Filter, cast beyond the declared schema, or apply business logic |
| **Silver** | Two datasets — valid and rejected, both partitioned, both reconciled | Drop a row without recording why |
| **dbt staging** | 1:1 renames and casts | Join, filter, aggregate |
| **dbt intermediate** | Derived measures, defined once | Persist anything nobody queries |
| **Gold marts** | Aggregates with a stated grain and business purpose | Re-derive a measure that intermediate already defines |
| **Redshift** | Sorted, distributed, granted | Enforce keys (that is dbt's job) |
