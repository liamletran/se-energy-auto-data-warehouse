# Nordic Energy Market Analytics Pipeline

> End-to-end analytics pipeline combining hourly ENTSO-E 
> to model energy cost exposure for Swedish industrial manufacturers.





## Business Context

The Nordic energy market is characterized by high volatility in electricity prices, driven by factors such as weather conditions, fuel costs, and grid constraints. For energy-intensive industries like automotive manufacturing, this volatility can lead to significant cost fluctuations and margin pressure. By building a data pipeline that ingests hourly market data, transforms it into actionable insights, and serves it through a dashboard, we can help manufacturers optimize their energy procurement strategies, identify cost-saving opportunities, and ultimately improve their financial performance in a competitive market.

## Live Dashboard
[]

## Architecture

The pipeline follows a modern **ELT (Extract-Load-Transform)** pattern:

1. **Ingestion (Extract & Load)**: 

Python + Dagster fetches hourly data from APIs, reshapes it (Wide to Long), and loads it into DuckDB Raw schema.

2. **Transformation (Transform)**: 

dbt takes over to clean, join, and model the data into Staging, Intermediate, and Mart layers.

3. **Orchestration**: 

Dagster coordinates the entire flow, ensuring dbt runs only after successful ingestion.

The pipeline is designed to be modular, scalable, and maintainable, allowing for easy addition of new data sources or transformation logic as business needs evolve.

![Pipeline](images/overview_pipeline-Page-1.gif)

## Stack
| Layer | Tool | Reason |
|---|---|---|
| Orchestration | Dagster | Coordinates partitioned hourly assets, manages retry logic, and triggers Slack alerts |
| Transformation | dbt + DuckDB + DBeaver | SQL-first, version-controlled modelling with a high-performance in-process OLAP warehouse |
| Serving | Evidence.dev | Code-based, Markdown+SQL driven BI layer for rapid, git-integrated dashboard deployment |
| Monitoring | Slack | Proactive, real-time alerting system linked with Dagster to minimize pipeline downtime |




## Data Sources
| Source | Frequency | Method |
|---|---|---|
| **ENTSO-E Transparency Platform** | Hourly | REST API | Raw electricity prices and generation mix for SE3 zone. |
| **Manual Reference Data** | Static | dbt seed (CSV) | Domain-specific lookup tables: OEE proxies, unit conversions, and NACE industry codes. |
| **SCB Industrial Production Index** | Monthly | REST API | Industrial Production Index (IPI) for manufacturing sectors. |
| **Frankfurter.dev Exchange Rates** | Daily | REST API | EUR to SEK exchange rates for cost modeling. |

## Key Design Decisions

- **ELT Architecture**: 
Separation of ingestion (Dagster) and transformation (dbt) allows for modularity and scalability. Ingestion focuses on fetching and loading raw data, while dbt handles all transformations and business logic.

- **Dagster**: Orchestrates the workflow, ensuring that data is ingested and transformed in the correct sequence. dbt handles all transformations, allowing for modular SQL development and testing. DuckDB serves as a local analytical warehouse, providing fast query performance for both dbt and the dashboard.

- **DuckDB as Warehouse**: 
DuckDB is used as a local analytical warehouse that Supports SQL and integrates well with dbt and thus is chosen for its simplicity and performance for local analytics.

- **Evidence.dev for Serving**: Evidence.dev is chosen for its seamless integration with SQL and Markdown, allowing for rapid dashboard development and deployment directly from the codebase, ensuring version control and collaboration.  

- **Proactive Monitoring**: Integration of Slack alerts with Dagster ensures that any pipeline failures are immediately communicated to the team, minimizing downtime and ensuring data reliability for decision-making.


## Definition of Done
- [x] 100% dbt tests pass 
- [x] Idempotent ingestion process
- [x] Source freshness monitoring configured
- [x] Live dashboard deployed at

## How to Run


