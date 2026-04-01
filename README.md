# Job Market Intelligence Platform

An end-to-end data pipeline that ingests tech job postings across North America, processes and normalizes the data, and produces salary and skill analytics through an interactive dashboard.

## Architecture

```
JSearch API --> S3 + Local Cache --> MySQL --> Snowflake --> dbt --> Streamlit Dashboard
                                       
```

## Tech Stack

| Layer | Tool |
|-------|------|
| Ingestion | Python, JSearch API (RapidAPI) |
| Raw Storage | AWS S3, Local JSON cache |
| Database | MySQL (OLTP) |
| Warehouse | Snowflake (OLAP) |
| Transformation | dbt (staging + mart models) |
| Orchestration | Apache Airflow (Dockerized) |
| Dashboard | Streamlit + Plotly |

## Pipeline Steps

1. **Fetch Jobs** - Queries 5 job roles from JSearch API, caches raw JSON locally and uploads to S3
2. **Extract Skills** - Scans job descriptions for 50+ tech skills and soft skills using keyword matching
3. **Load to Snowflake** - Transfers normalized data from MySQL to Snowflake with duplicate prevention
4. **dbt Run** - Builds staging models (salary normalization, hourly to annual conversion) and 5 analytics marts
5. **dbt Test** - Validates data quality (unique keys, not-null constraints, accepted values)

## Analytics Marts

- **Salary by Role** - Average salary breakdown by experience level
- **Top Skills** - Most in-demand skills with percentage across all postings
- **Jobs by Location** - Geographic distribution with remote job counts
- **Jobs by Company** - Top hiring companies with salary averages
- **Estimated Salary** - 3-tier fallback: actual salary, level average, overall average

## Data Quality

- Duplicate prevention at ingestion (source_url check) and warehouse (all 3 tables)
- Salary normalization: hourly rates (below 500) converted to annual (multiply by 2080)
- Experience level extraction from job titles
- dbt tests on all critical columns

## Project Structure

```
job-market-pipeline/
├── ingestion/          # API fetcher, skill extractor, Snowflake loader
├── config/             # Centralized config (gitignored)
├── storage/            # MySQL schema
├── transformation/     # dbt project (staging + marts)
├── orchestration/      # Airflow DAG + Docker setup
├── serving/            # Streamlit dashboard
└── data/raw/           # Cached API responses (gitignored)
```

## Setup

1. Clone the repo
2. Create `config/config.py` with your API key, MySQL, Snowflake, and S3 credentials
3. Run `storage/schema.sql` in MySQL
4. Set up Snowflake warehouse and database
5. Configure dbt: `~/.dbt/profiles.yml`
6. Start Airflow: `cd orchestration && docker compose up -d`
7. Run dashboard: `streamlit run serving/dashboard.py`
