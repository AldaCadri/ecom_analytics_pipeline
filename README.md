# E-Commerce Analytics Pipeline & Dashboard
End-to-end e-commerce analytics pipeline for Amazon purchase &amp; survey data.
**Analysis of 1.8M Amazon Purchases by 5,000+ U.S. Users**  
📆 2018–2022 | 🔍 Linked Demographics | 💡 Real-World Insights

## Project Summary

The project demonstrates an end-to-end analytics solution using real-world e-commerce data from Amazon, integrating multiple file formats (CSV, JSON), enriching with demographic APIs, and transforming it using Snowflake and dbt. 

## Tech Stack:  

- **Snowflake** – Storage, compute, warehouse orchestration.  
- **dbt** – SQL-based transformations.  
- **Python** – API ingestion, preprocessing.  
- **Streamlit** – Interactive dashboard UI.  
- **GitHub** – Version control & collaboration.

---

## Data Sources

This project uses the **Open E-Commerce 1.0** dataset ([Harvard Dataverse link](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/YGLYDY)), a first-of-its-kind public dataset containing detailed Amazon.com purchase histories from **5,027 U.S. consumers** between **2018–2022**, totaling over **1.8 million transactions**.  

The dataset was crowdsourced via an online survey, with participants’ informed consent, and links each purchase to rich demographic, lifestyle data.   

Additionally, U.S. Census **American Community Survey (ACS)** data ([data.census.gov](https://data.census.gov/)) is integrated via API to append **state-level median income** and **population** for 2018–2023.  

| Dataset | File Name | Format | Description | Source |
|---------|-----------|--------|-------------|--------|
| Amazon Purchases | `amazon-purchases.csv` | CSV | 1.85M orders with date, product, price, quantity, shipping state | [Harvard Dataverse](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/YGLYDY) |
| Survey Data | `survey.csv` | CSV | Demographic, household, income, education, lifestyle data | [Harvard Dataverse](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/YGLYDY) |
| Survey Metadata | `fields.csv` | CSV | Column definitions and survey question metadata | [Harvard Dataverse](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/YGLYDY) |
| ACS State Demographics | API | JSON | Median household income & population by state/year (2018–2023) | [data.census.gov](https://data.census.gov/) |



## Architecture



```mermaid
flowchart LR
  A["Raw Data<br>CSV · JSON · API"] --> B["Staging Layer<br>Clean · Cast · Dedup"]
  B --> C["Refinement Layer<br>Star Schema: Fact + Dimensions"]
  C --> D["Delivery Layer<br>Curated Marts"]
  D --> E["Streamlit Dashboard<br>Live Snowflake Queries"]

  classDef raw fill:#e3f2fd,stroke:#1e88e5,color:#0d47a1;
  classDef stg fill:#e8f5e9,stroke:#43a047,color:#1b5e20;
  classDef ref fill:#fff8e1,stroke:#f9a825,color:#3e2723;
  classDef del fill:#fce4ec,stroke:#d81b60,color:#880e4f;
  classDef dash fill:#ede7f6,stroke:#5e35b1,color:#311b92;

  class A raw; class B stg; class C ref; class D del; class E dash; 

```
---

## Star Schema

![Star Schema](images/star_schema.png)

## DAG Pipeline

The **dbt DAG (Directed Acyclic Graph)** represents the complete flow of data from ingestion to delivery.  
It starts with **raw source tables**, moves into the **staging layer** where data is cleaned and standardized, then into the **refinement layer** where a **star schema** is implemented, and finally into the **delivery layer** where business-ready marts are produced.  
This visual makes it easy to trace dependencies, ensuring transparency and maintainability.

![Project Pipeline](images/Project_dag.png)


## Layer Breakdown

### **Staging Layer**
| Model | Purpose | Link |
|-------|---------|------|
| `stg_amazon_purchases` | Cleans and type-casts order data, removes duplicates | [View SQL](models/staging/stg_amazon_purchases.sql) |
| `stg_survey` | Normalizes survey fields, sets flag boolean | [View SQL](models/staging/stg_survey.sql) |
| `stg_state_demographics` | Parses ACS JSON payloads into typed columns | [View SQL](models/staging/stg_state_demographics.sql) |
| `stg_state_codes` | Minimal mapping of postal/state/FIPS | [View SQL](models/staging/stg_state_codes.sql) |

---

### **Refinement Layer (Star Schema)**

#### **Fact Table**
- `fct_orders` – Order grain, links all dimensions | [View SQL](models/refinement/fct_orders.sql)

**Dimensions**
| Model | Purpose | Link |
|-------|---------|------|
| `dim_user` | Customer demographics, household & account info | [View SQL](models/refinement/dim_user.sql) |
| `dim_state` | Canonical state lookup (postal, name, FIPS) | [View SQL](models/refinement/dim_state.sql) |
| `dim_product` | SKU details, gift card flag | [View SQL](models/refinement/dim_product.sql) |
| `dim_date` | Time dimension for analysis | [View SQL](models/refinement/dim_date.sql) |
| `dim_state_demographics` | Median income & population by state/year | [View SQL](models/refinement/dim_state_demographics.sql) |

**Enriched Orders**
- `ref_orders_enriched` – Joins staging data with demographics and computed metrics | [View SQL](models/refinement/ref_orders_enriched.sql)

---

### **Delivery Layer (Marts)**
| Model | Purpose | Link |
|-------|---------|------|
| `mart_sales_by_state_m_y` | Monthly sales by state | [View SQL](models/delivery/mart_sales_by_state_m_y.sql) |
| `mart_sales_by_category_m_y` | Monthly sales by category | [View SQL](models/delivery/mart_sales_by_category_m_y.sql) |
| `mart_customer_segment_metrics` | Revenue by demographic segments | [View SQL](models/delivery/mart_customer_segment_metrics.sql) |
| `mart_cohort_retention` | Customer retention over time | [View SQL](models/delivery/mart_cohort_retention.sql) |
| `mart_top_products` | Top 50 products by revenue and units | [View SQL](models/delivery/mart_top_products.sql) |
| `mart_revenue_by_income_state` | Revenue & order count by income bracket/state | [View SQL](models/delivery/mart_revenue_by_income_state.sql) |
| `mart_revenue_vs_income_state_year` | Revenue vs median income per state/year | [View SQL](models/delivery/mart_revenue_vs_income_state_year.sql) |

---

## Dashboard Features
The interactive **Streamlit dashboard** is deployed and accessible here: [View Live Dashboard](https://aldacadri-ecom-analytics-pipeline-dashboard-appapp-test-oawfve.streamlit.app/)
It contains **5 interactive tabs**:

1. **Sales Overview** – Revenue trends, sales by state/year.  
2. **Category Performance** – Revenue & Avg. Order Value by category.  
3. **Customer Insights** – Revenue by age, income, and other demographics.  
4. **Cohort Analysis** – Loyalty tracking over months.  
5. **Revenue vs Income** – Correlation between state income & purchasing.  

---

## Example Business Questions Answered
- Do wealthier states generate more revenue?  
- Are lower-income states showing purchasing growth?  
- How does median household income correlate with revenue?  
- Which product categories drive the highest Avg. Order Value?  
- How does customer retention vary by cohort?  

---

## Sensitive Data & Secrets
- All Snowflake credentials are stored in `.env` / `secrets.toml` (not committed to GitHub).
- Example `.env` template is included for safe local setup.

---
## Notes & Recommendations
- **Incomplete Data:** Avoid using partial 2023–2024 records for trend analysis.  
- **More Filters:** Add category, income, and product type filters in more dashboard tabs.  
- **Incremental Models:** Use for large marts (e.g., cohorts, top products) with late-arriving data logic.  
- **Automated Data Quality:** Send dbt test alerts to Slack/email for schema drift, null spikes, or duplicates.  
- **Automated Orchestration:** Schedule builds with dbt Cloud or Snowflake Tasks.  
- **Performance & Cost:** Pre-aggregate high-query marts with materialized views; monitor usage via `QUERY_HISTORY`.  
- **Advanced Analytics:** CLV prediction, market basket analysis, price sensitivity, forecasting, and anomaly detection.  

---

## Future Work
- Maintain **full-year snapshots** for consistent reporting.  
- Improve category mapping to reduce “UNKNOWN” values and segment digital/gift card orders.  
- Optimize performance with warehouse separation, clustering, and materialized views.  
- Add advanced filtering, drill-through, and seasonal trend visuals to the dashboard.  
- Introduce churn forecasting and executive-level summaries.  
- Enrich with external datasets for added business context.  

---

## Full Documentation
For detailed transformation logic, schema tests, and SQL models, see the full PDF in the repo or click [here](Ecommerce_Analytics_Full_Documentation.pdf) 
