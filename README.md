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


| Dataset | Description |
|--------|-------------|
| `amazon-purchases.csv` | 1.85M orders (2018–2024) |
| `survey.csv` | User demographics |
| `fields.csv` | Metadata |
| U.S. Census API (json) | State income & population (2018–2023) |

## Architecture
Raw Data → Staging Layer → Refinement Layer (Star Schema) → Delivery Layer (Marts) → Streamlit Dashboard

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


