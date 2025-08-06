# dashboard_app/app.py

import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import altair as alt
import calendar
import numpy as np 
 

# —————————————————————————————————————————————————————
@st.cache_resource
def get_session():
    creds = st.secrets["snowflake"]
    return Session.builder.configs(creds).create()

session = get_session()
st.set_page_config(layout="wide")
st.title("📊 E-Commerce Analytics Dashboard")
st.markdown(
    "#### Analysis of 1.8M Amazon Purchases by 5,000+ U.S. Users with Linked Demographics and Behavioral Insights"
)
with st.expander("ℹ️ About this dashboard"):
    st.markdown(
        """
        📦 _This dashboard analyzes 5 years of crowdsourced U.S. Amazon purchase data (2018–2022), 
        contributed by over 5,000 users through a structured survey. Each transaction is linked to 
        user demographics, enabling powerful trend, income, and behavior analyses across states and 
        customer groups._

        ⚠️ _Note: Data for 2023–2024 is incomplete and ongoing._
        """
    )

# —————————————————————————————————————————————————————
# Sidebar Filters
years = [r[0] for r in session.table("mart_sales_by_state_m_y").select("YEAR").distinct().order_by("YEAR").collect()]
states = [r[0] for r in session.table("mart_sales_by_state_m_y").select("STATE_NAME").distinct().order_by("STATE_NAME").collect()]

st.sidebar.header("Filters")
selected_years = st.sidebar.multiselect("Year", years, default=years)
selected_states = st.sidebar.multiselect("State", states, default=states)

# —————————————————————————————————————————————————————
# Cache data loading functions
# These functions load data from Snowflake 
@st.cache_data(ttl=3600)
def load_state_month(selected_years, selected_states):
    df = (
        session.table("mart_sales_by_state_m_y")
        .filter(col("YEAR").isin(selected_years))
        .filter(col("STATE_NAME").isin(selected_states))
        .select("YEAR", "MONTH", "STATE_NAME", "TOTAL_REVENUE")
        .to_pandas()
    )
    df.columns = df.columns.str.lower()
    df["month"] = pd.to_numeric(df["month"], errors="coerce").fillna(1).astype(int)
    df["year"] = pd.to_numeric(df["year"], errors="coerce").fillna(0).astype(int)
    df["month_year_label"] = pd.to_datetime(df[["year", "month"]].assign(day=1))
    return df

@st.cache_data(ttl=3600)
def load_category_month(selected_years):
    df = (
        session.table("mart_sales_by_category_m_y")
        .filter(col("YEAR").isin(selected_years))
        .select("YEAR", "MONTH", "CATEGORY", "TOTAL_REVENUE")
        .to_pandas()
    )
    df.columns = df.columns.str.lower()
    df["month"] = pd.to_numeric(df["month"], errors="coerce").fillna(1).astype(int)
    df["year"] = pd.to_numeric(df["year"], errors="coerce").fillna(0).astype(int)
    df["month_year_label"] = pd.to_datetime(df[["year", "month"]].assign(day=1))
    return df

@st.cache_data(ttl=3600)
def load_top_products(selected_years):
    return (
        session.table("mart_top_products")
        .filter(col("YEAR").isin(selected_years))
        .select("PRODUCT_KEY", "CATEGORY", "TOTAL_QUANTITY", "TOTAL_REVENUE")
        .sort(col("TOTAL_REVENUE").desc())
        .limit(10)
        .to_pandas()
        .rename(columns={"TOTAL_REVENUE": "total_sales", "TOTAL_QUANTITY": "total_quantity"})
        .rename(columns=str.lower)
    )

@st.cache_data(ttl=3600)
def load_segments(selected_years):
    return session.table("mart_customer_segment_metrics").filter(col("YEAR").isin(selected_years)).to_pandas().rename(columns=str.lower)

@st.cache_data(ttl=3600)
def load_cohorts(selected_years):
    df = session.table("mart_cohort_retention").to_pandas()
    df.columns = df.columns.str.lower()
    df["cohort_month"] = pd.to_datetime(df["cohort_month"])
    return df[df["cohort_year"].isin(selected_years)]

# —————————————————————————————————————————————————————
# Load Data
df_state = load_state_month(selected_years, selected_states)
df_cat = load_category_month(selected_years)
df_top = load_top_products(selected_years)
df_segments = load_segments(selected_years)
df_cohort = load_cohorts(selected_years)

# —————————————————————————————————————————————————————
# Layout Tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Sales Overview", "📦 Category Performance", "👥 Customer Insights", "🔄 Cohort Analysis", "📊 Revenue & Income Trends"])

# —————————————————————————————————————————————————————
with tab1:
    st.caption("Note: Sales from years with incomplete data (e.g., 2023–2024) are included but may skew results.")

    # Create month names
    df_state["month_num"] = df_state["month"]
    df_state = df_state[df_state["month"].between(1, 12)]
    df_state["month_num"] = df_state["month"]
    df_state["month_name"] = df_state["month_num"].apply(lambda x: calendar.month_abbr[x])

    # Prepare the base chart with month order
    month_order = list(calendar.month_abbr)[1:]  # ['Jan', ..., 'Dec']
    
    st.subheader("📌 High-Level KPIs")
    total_sales = df_state["total_revenue"].sum()
    avg_monthly = df_state.groupby(["year", "month"])["total_revenue"].sum().mean()
    unique_states = df_state["state_name"].nunique()


    c1, c2, c3 = st.columns(3)
    c1.metric("Total Sales", f"${total_sales:,.0f}")
    c2.metric("Avg. Monthly Sales", f"${avg_monthly:,.0f}")
    c3.metric("States Shown", f"{unique_states}")

    
    
    st.subheader("📈 Monthly Sales Trend")
    st.caption("📌 Monthly sales trend across all selected years. Gray bars show total revenue per month, while colored lines show year-over-year trends.")

    # Line chart: revenue by year
    line = alt.Chart(df_state).mark_line(point=True).encode(
        x=alt.X("month_name:N", title="Month", sort=month_order),
        y=alt.Y("sum(total_revenue):Q", title="Total Sales", axis=alt.Axis(format=",.0f")),
        color=alt.Color("year:N", title="Year")
    )

    # Bar chart: total revenue by month (all years)
    df_bar = df_state.groupby("month_name")["total_revenue"].sum().reindex(month_order).reset_index()
    bar = alt.Chart(df_bar).mark_bar(opacity=0.2, color="gray").encode(
        x=alt.X("month_name:N", title="Month", sort=month_order),
        y=alt.Y("total_revenue:Q", title="Total Sales", axis=alt.Axis(format=",.0f"))
    )

    # Combine both
    combo_chart = alt.layer(bar, line).resolve_scale(y='shared').properties(width=800, height=400)

    st.altair_chart(combo_chart, use_container_width=True)

    st.subheader("🏷️ Sales by State")

    # Interactive selection for Top N states
    top_n_states = st.slider("Select Top N States by Revenue", min_value=5, max_value=53, value=15)

    # Group by state and year to prevent duplicated bars
    df_state_grouped = (
        df_state.groupby(["state_name", "year"], as_index=False)
        .agg(total_revenue=("total_revenue", "sum"))
    )

    # Compute top N states
    top_states = (
        df_state.groupby("state_name")["total_revenue"]
        .sum()
        .nlargest(top_n_states)
        .index.tolist()
    )

    df_state_top = df_state_grouped[df_state_grouped["state_name"].isin(top_states)]

    # Sort by total revenue
    state_order = (
        df_state_top.groupby("state_name")["total_revenue"]
        .sum()
        .sort_values(ascending=False)
        .index.tolist()
    )

    # Altair stacked bar chart
    bar_chart = alt.Chart(df_state_top).mark_bar().encode(
        x=alt.X("state_name:N", sort=state_order, title="State"),
        y=alt.Y("total_revenue:Q", title="Total Revenue", axis=alt.Axis(format="~s")),
        color=alt.Color("year:N", title="Year"),
        tooltip=[alt.Tooltip("state_name", title="State"),
        alt.Tooltip("year", title="Year"),
        alt.Tooltip("total_revenue", title="Total Revenue", format=",.0f")]
    ).properties(
        width=800,
        height=400
    )

    st.altair_chart(bar_chart, use_container_width=True)




    st.subheader("📈 Year-over-Year Sales Growth")

    # 1. Compute Year-over-Year Growth
    yoy_data = df_state.groupby("year")["total_revenue"].sum().pct_change() * 100
    yoy_df = yoy_data.fillna(0).reset_index()
    yoy_df.columns = ["year", "growth"]

    # 2. Create Base Chart
    base = alt.Chart(yoy_df).encode(
        x=alt.X("year:O", title="Year"),
        y=alt.Y("growth:Q", title="YoY Growth (%)"),
        tooltip=[
            alt.Tooltip("year:O", title="Year"),
            alt.Tooltip("growth:Q", title="Growth %", format=".2f")
        ]
    )

    # 3. Bar chart with color logic
    bars = base.mark_bar().encode(
        color=alt.condition(
            alt.datum.growth >= 0,
            alt.value("green"),
            alt.value("red")
        )
    )

    # 4. Add text labels on top
    labels = base.mark_text(
        align="center",
        baseline="bottom",
        dy=-5  # vertical offset
    ).encode(
        text=alt.Text("growth:Q", format=".1f")
    )

    # 5. Add 0% reference rule
    zero_line = alt.Chart(pd.DataFrame({"y": [0]})).mark_rule(strokeDash=[3, 3], color="black").encode(
        y="y"
    )

    # 6. Combine all layers
    yoy_chart = (bars + labels + zero_line).properties(
        width=700,
        height=400
    )

    # 7. Show chart in Streamlit
    st.altair_chart(yoy_chart, use_container_width=True)

    # 8. Caption for context
    st.caption("📌 Year-over-Year growth in total revenue. Green bars indicate increase, red bars indicate decline compared to previous year.")



# —————————————————————————————————————————————————————
with tab2:
    st.subheader("🏷️ Top Categories by Yearly Sales")
    st.caption("Note: Sales from years with incomplete data (e.g., 2023–2024) are included but may skew results.")

    # Load filtered category data
    df_cat = (
        session.table("mart_sales_by_category_m_y")
        .filter(col("YEAR").isin(selected_years))
        .select("YEAR", "MONTH", "CATEGORY", "TOTAL_REVENUE", "ORDERS_COUNT")
        .to_pandas()
    )

    df_cat.columns = df_cat.columns.str.upper()

    # Top N filter in-tab
    top_n_tab = st.slider("Top N Categories", min_value=3, max_value=15, value=6)

    # Compute top categories
    top_categories = (
        df_cat.groupby("CATEGORY")["TOTAL_REVENUE"]
        .sum()
        .nlargest(top_n_tab)
        .index.tolist()
    )

    # Group categories by total revenue
    cat_revenue = (
        df_cat.groupby("CATEGORY", as_index=False)["TOTAL_REVENUE"]
        .sum()
        .sort_values("TOTAL_REVENUE", ascending=False)
    )

    # Separate 'UNKNOWN' from other categories
    known_cats = cat_revenue[cat_revenue["CATEGORY"] != "UNKNOWN"]
    unknown_cat = cat_revenue[cat_revenue["CATEGORY"] == "UNKNOWN"]

    # Combine known first, unknown last
    ordered_categories = pd.concat([known_cats, unknown_cat])["CATEGORY"].tolist()

    # Format labels (optional)
    category_labels = {
        cat: ("Unknown (Unclassified)" if cat == "UNKNOWN" else cat)
        for cat in ordered_categories
    }

    # Default: top N excluding 'UNKNOWN'
    default_cats = known_cats["CATEGORY"].head(top_n_tab).tolist()

    # Multiselect with display labels
    selected_cats = st.multiselect(
        "Select categories to display",
        options=ordered_categories,
        format_func=lambda x: category_labels[x],
        default=default_cats
    )


    filtered_yearly = df_cat[df_cat["CATEGORY"].isin(selected_cats)].groupby(["YEAR", "CATEGORY"], as_index=False)["TOTAL_REVENUE"].sum()
    filtered_all = df_cat[df_cat["CATEGORY"].isin(selected_cats)]

    # Chart 1: Top categories by yearly sales
    sales_chart = alt.Chart(filtered_yearly).mark_bar().encode(
        x=alt.X("YEAR:O", title="Year"),
        y=alt.Y("TOTAL_REVENUE:Q", title="Total Revenue",axis=alt.Axis(format="~s")),
        color=alt.Color("CATEGORY:N", title="Category"),
        tooltip=[alt.Tooltip("YEAR", title="Year"),
        alt.Tooltip("CATEGORY", title="Category"),
        alt.Tooltip("TOTAL_REVENUE", title="Total Revenue", format=",.0f")]
    ).properties(width=800, height=400)
    st.altair_chart(sales_chart, use_container_width=True)

    # Chart 2: Avg. Order Value by Category
    st.subheader("💰 Avg. Order Value by Category")
    df_aov = (
        filtered_all.groupby("CATEGORY", as_index=False)
        .agg({"TOTAL_REVENUE": "sum", "ORDERS_COUNT": "sum"})
    )
    df_aov["AOV"] = df_aov["TOTAL_REVENUE"] / df_aov["ORDERS_COUNT"]

    aov_chart = alt.Chart(df_aov).mark_bar().encode(
        x=alt.X("CATEGORY:N", sort="-y", title="Category"),
        y=alt.Y("AOV:Q", title="Average Order Value",axis=alt.Axis(format="~s")),
        tooltip=[alt.Tooltip("CATEGORY", title="Category"),
                 alt.Tooltip("AOV", title="AOV", format=",.2f")]
    ).properties(width=800, height=400)
    st.altair_chart(aov_chart, use_container_width=True)

    # Chart 3: Underperforming Categories
    st.subheader("📉 Underperforming Categories")
    df_low = (
        df_cat.groupby("CATEGORY", as_index=False)
        .agg({"TOTAL_REVENUE": "sum"})
        .sort_values("TOTAL_REVENUE")
        .head(5)
    )

    low_chart = alt.Chart(df_low).mark_bar().encode(
        x=alt.X("CATEGORY:N", sort="-y", title="Category"),
        y=alt.Y("TOTAL_REVENUE:Q", title="Total Revenue"),
        tooltip=["CATEGORY", "TOTAL_REVENUE"]
    ).properties(width=800, height=400)
    st.altair_chart(low_chart, use_container_width=True)



# —————————————————————————————————————————————————————
with tab3:
    st.subheader("🧍 Customer Segment Insights")

    st.markdown("""
    👥 Explore how different **age** and **income** segments contribute to orders, revenue, and average order value.  
    ⚠️ **Note:** Incomplete data in later years (e.g., 2023–2024) may skew results.
    """)

    # Multiselect: Income Brackets
    income_filter = st.multiselect(
        "💰 Select Income Bracket(s)", 
        options=sorted(df_segments["income_bracket"].unique()),
        default=sorted(df_segments["income_bracket"].unique())
    )

    # Filter by year (from sidebar) and selected income brackets
    filtered_seg = df_segments[
        (df_segments["year"].isin(selected_years)) &
        (df_segments["income_bracket"].isin(income_filter))
    ]

    # Segment summary
    st.markdown("### 📌 Segment Summary for Selected Year(s)")
    st.caption(f"Showing data for {len(selected_years)} year(s): {', '.join(map(str, selected_years))}")

    col3, col4, col5 = st.columns(3)
    col3.metric("🧾 Total Orders", f"{int(filtered_seg['orders_count'].sum()):,}")
    col4.metric("📦 Total Revenue", f"${filtered_seg['total_revenue'].sum():,.2f}")
    col5.metric("💳 Avg. Order Value", f"${filtered_seg['avg_order_value'].mean():,.2f}")

    # Chart 1: Avg. Order Value by Age Group
    st.subheader("💳 Avg. Order Value by Age Group")

    chart_aov = alt.Chart(filtered_seg).mark_bar().encode(
        x=alt.X("age_group:N",title="Age Group", sort=[
            "18 - 24 years", "25 - 34 years", "35 - 44 years",
            "45 - 54 years", "55 - 64 years", "65 years and over"
        ]),
        y=alt.Y("avg_order_value:Q", title="Avg. Order Value", axis=alt.Axis(format="~s")),
        color=alt.Color("year:N", title="Year") if len(selected_years) > 1 else alt.value("#4C78A8"),
        tooltip=[alt.Tooltip("year", title="Year"),
                 alt.Tooltip("age_group", title="Age Group"),
                 alt.Tooltip("income_bracket", title="Income Bracket"),
                 alt.Tooltip("avg_order_value", title="Avg. Order Value", format=",.2f")]
    ).properties(width=750, height=400)

    st.altair_chart(chart_aov, use_container_width=True)

    # Chart 2: Revenue by Income Bracket
    st.subheader("💵 Revenue by Income Bracket")

    grouped_revenue = (
    filtered_seg.groupby(["year", "income_bracket"], as_index=False)["total_revenue"]
    .sum()
)
    chart_revenue = alt.Chart(grouped_revenue).mark_bar().encode(
    x=alt.X("income_bracket:N", title="Income Bracket"),
    y=alt.Y("total_revenue:Q", title="Total Revenue", axis=alt.Axis(format="~s")),
    color=alt.Color("year:N", title="Year"),
    tooltip=[
        alt.Tooltip("year", title="Year"),
        alt.Tooltip("income_bracket", title="Income Bracket"),
        alt.Tooltip("total_revenue", title="Total Revenue", format=",.2f")]
     
).properties(width=750, height=400)

    st.altair_chart(chart_revenue, use_container_width=True)

    # Chart 3: Orders by Age Group
    st.subheader("📊 Orders by Age Group")

    grouped_orders = (
    filtered_seg.groupby(["year", "age_group"], as_index=False)["orders_count"]
    .sum()
)
    chart_orders = alt.Chart(grouped_orders).mark_bar().encode(
    x=alt.X("age_group:N", sort=[
        "18 - 24 years", "25 - 34 years", "35 - 44 years",
        "45 - 54 years", "55 - 64 years", "65 years and over"
    ], title="Age Group"),
    y=alt.Y("orders_count:Q", title="Total Orders", axis=alt.Axis(format="~s")),
    color=alt.Color("year:N", title="Year"),
    tooltip=[
        alt.Tooltip("year", title="Year"),
        alt.Tooltip("age_group", title="Age Group"),
        alt.Tooltip("orders_count", title="Number of Orders", format=",.0f")]
        
).properties(width=750, height=400)

    st.altair_chart(chart_orders, use_container_width=True)

    # Optional expandable raw data table
    with st.expander("🔍 View Full Customer Segments Table"):
        st.dataframe(
            filtered_seg.style.format({
                "total_revenue": "{:,.2f}",
                "avg_order_value": "{:,.2f}"
            }),
            use_container_width=True
        )



# —————————————————————————————————————————————————————
with tab4:
    st.markdown("## 🔁 Cohort Retention Analysis")

    # ——— Explanation ———
    st.markdown("""
    ### 🧠 What Is Cohort Retention?

    **Cohort analysis** tracks groups of users based on their first order month.  
    This helps visualize how many users return after 1, 2, 3... months.
    """)

    # ——— Load data ———
    df_cohort = session.table("mart_cohort_retention").to_pandas()
    df_cohort.columns = [col.lower() for col in df_cohort.columns]

    df_cohort["cohort_month"] = pd.to_datetime(df_cohort["cohort_month"])
    df_cohort["cohort_label"] = df_cohort["cohort_month"].dt.strftime("%Y-%m")

    # ——— Filters ———
    min_year = int(df_cohort["cohort_month"].dt.year.min())
    max_year = int(df_cohort["cohort_month"].dt.year.max())

    year_range = st.slider("📆 Select Cohort Year Range", min_year, max_year, (min_year, max_year))
    month_range = st.slider("🕒 Select Months After Range", 0, int(df_cohort["months_after"].max()), (0, 12))

    df_filtered = df_cohort[
        (df_cohort["cohort_month"].dt.year.between(year_range[0], year_range[1])) &
        (df_cohort["months_after"].between(month_range[0], month_range[1]))
    ]

    # ——— Top 3 Cohorts ———
    cohort_sizes = df_filtered.groupby("cohort_label")["cohort_size"].first().reset_index()
    avg_ret = df_filtered.groupby("cohort_label")["retention_pct"].mean().reset_index()
    avg_ret = avg_ret.merge(cohort_sizes, on="cohort_label")
    avg_ret = avg_ret[avg_ret["cohort_size"] >= 20]
    top_cohorts = avg_ret.sort_values("retention_pct", ascending=False).head(3)["cohort_label"].tolist()

    df_top = df_filtered[df_filtered["cohort_label"].isin(top_cohorts)]

    # ——— Retention Curve ———
    line_chart = alt.Chart(df_top).mark_line(point=True).encode(
        x=alt.X("months_after:O", title="Months After"),
        y=alt.Y("retention_pct:Q", title="% Retained"),
        color=alt.Color("cohort_label:N", title="Cohort"),
        tooltip=["cohort_label", "months_after", "retention_pct"]
    ).properties(
        title="📈 Retention Curve of Top 3 Cohorts",
        width=850,
        height=400
    )
    st.altair_chart(line_chart, use_container_width=True)

    # ——— Automated Interpretation ———
    example_point = df_top[df_top["months_after"] == df_top["months_after"].max()].iloc[0]
    st.markdown("### 📘 How to Interpret the Retention Curve")
    st.markdown(f"""
    - Each line shows how a **cohort** (group of users who made their first order in the same month) retained over time.
    - **X-axis**: Number of months after the first order.
    - **Y-axis**: What **% of original users** from that cohort returned that many months later.
    - For example, in cohort **{example_point['cohort_label']}**, **{round(example_point['retention_pct'], 1)}%** of users were still active **{example_point['months_after']} months** after their first order.
    - Retention typically drops after the first month, but cohorts with better retention show flatter curves.
    - Only cohorts with **at least 20 users** are included to ensure meaningful trends.
    """)

    # ——— Summary Table ———
    st.markdown("### 📌 Retention Summary for Top Cohorts (Months 1–3)")
    summary = df_top[df_top["months_after"].between(1, 3)]
    summary_table = summary.pivot_table(
        index="cohort_label",
        columns="months_after",
        values="retention_pct",
        aggfunc="mean"
    ).round(1).rename_axis(columns=None).reset_index()
    summary_table.columns = ["Cohort"] + [f"Month {m}" for m in summary_table.columns[1:]]
    st.dataframe(summary_table, use_container_width=True)

    # ——— Enhanced Pivot Table ———
    st.markdown("### 🧾 Cohort Retention Table (User Count & Retention %)")
    user_pivot = df_top.pivot_table(
        index="cohort_label",
        columns="months_after",
        values="active_users",
        aggfunc="sum",
        fill_value=0
    )
    retention_pivot = df_top.pivot_table(
        index="cohort_label",
        columns="months_after",
        values="retention_pct",
        aggfunc="mean",
        fill_value=0
    ).round(1)

    combined = user_pivot.astype(str) + " users (" + retention_pivot.astype(str) + "%)"
    st.dataframe(combined, use_container_width=True)

    # ——— Final Insights ———
    st.markdown(f"""
    ### 🔍 Insights

    - 🏆 **Top Cohorts**: {', '.join(top_cohorts)} had the highest average retention.
    - 📉 Most user drop-offs occur after Month 1.
    - 🧪 This analysis only includes cohorts with **at least 20 users** to ensure reliability.
    - 🧭 Use the filters above to explore trends by cohort year or retention window.
    - 📈 Flatter curves signal better long-term loyalty.
    """)

with tab5:
    st.markdown("""
### 🧠 Insights: Income vs Revenue Analysis by State

This tab explores the relationship between **median household income** and **consumer revenue generation** across U.S. states, with two perspectives:

- **Per Capita Revenue vs. Median Income (Bubble Size = Population Group):**  
  This scatterplot shows state-level purchasing behavior normalized by population. Each bubble represents a state, with larger bubbles for more populous states. A positive correlation and upward trend indicate that **higher-income states tend to generate more revenue per person**, revealing stronger purchasing power.

- **Nationwide Trends Over Time (2018–2022):**  
  This line chart compares national **normalized trends** in median income and total revenue. It shows that both income and spending increased over time, but not always in sync.

💡 *Use the dropdown to select a year for state-level analysis. This will filter the scatterplot to show states only for the selected year, helping you identify annual variations.*

⚠️ **Note:** Data for **2023 and 2024** is **incomplete** and do not reflect full-year values.
""")

    # Load data from mart_revenue_vs_income_state_year
    df_mart = session.table("mart_revenue_vs_income_state_year").to_pandas()
    df_mart.columns = [col.lower() for col in df_mart.columns]

    # ——— Year Filter ———
    unique_years = sorted(df_mart["year"].dropna().unique())
    selected_year = st.selectbox("📆 Select Year", unique_years, index=len(unique_years) - 3, key="tab5_year")
    df_filtered = df_mart[df_mart["year"] == selected_year].copy()

    # ——— Define population group bins ———
    def population_group(pop):
        if pop < 5_000_000:
            return "< 5M"
        elif pop < 10_000_000:
            return "5M–10M"
        elif pop < 20_000_000:
            return "10M–20M"
        else:
            return "20M+"

    df_filtered["population_group"] = df_filtered["total_population"].apply(population_group)

    # ——— Chart: Per Capita Revenue vs Income ———
    st.markdown("### 📍 State-Level: Per Capita Revenue vs. Median Income")

    # Calculate per capita revenue
    df_filtered["per_capita_revenue"] = df_filtered["total_revenue"] / df_filtered["total_population"]

    # Define bubble size scale
    size_scale = alt.Scale(
        domain=["< 5M", "5M–10M", "10M–20M", "20M+"],
        range=[100, 300, 600, 900]
    )

    # Calculate trendline for correlation
    corr = df_filtered[["median_household_income", "per_capita_revenue"]].dropna().corr().iloc[0, 1]

    trend_data = df_filtered[["median_household_income", "per_capita_revenue"]].dropna()
    coef = np.polyfit(trend_data["median_household_income"], trend_data["per_capita_revenue"], 1)
    trendline = pd.DataFrame({
        "median_household_income": np.linspace(trend_data["median_household_income"].min(), trend_data["median_household_income"].max(), 100)
    })
    trendline["per_capita_revenue"] = coef[0] * trendline["median_household_income"] + coef[1]

    scatter_pc = alt.Chart(df_filtered).mark_circle(opacity=0.7).encode(
        x=alt.X("median_household_income:Q", title="Median Household Income"),
        y=alt.Y("per_capita_revenue:Q", title="Per Capita Revenue", axis=alt.Axis(format="~s")),
        size=alt.Size("population_group:N", scale=size_scale, title="Population Group"),
        color=alt.value("#1f77b4"),
        tooltip=[
            alt.Tooltip("state_name", title="State"),
            alt.Tooltip("total_population", title="Total Population", format=",.0f"),
            alt.Tooltip("per_capita_revenue", title="Revenue per capita", format=",.3f"),
            alt.Tooltip("median_household_income", title="Median Household Income", format=",.2f")
        ]
    )

    line = alt.Chart(trendline).mark_line(color="red").encode(
        x="median_household_income",
        y="per_capita_revenue"
    )

    # Annotate correlation
    annotation = alt.Chart(pd.DataFrame({
        "x": [trend_data["median_household_income"].min()],
        "y": [trend_data["per_capita_revenue"].max()],
        "text": [f"Correlation (r): {corr:.2f}"]
    })).mark_text(align="left", dx=10, dy=-10, color="black").encode(
        x="x:Q",
        y="y:Q",
        text="text:N"
    )

    final_chart = (scatter_pc + line + annotation).properties(
        title="State-Level: Per Capita Revenue vs. Median Income (Bubble Size = Population Group)",
        width=850,
        height=500
    )

    st.altair_chart(final_chart, use_container_width=True)


    # ——— Chart 2: Nationwide Trend with Sidebar Year Filter ———
    st.markdown("### 🕒 Nationwide Trends: Median Income vs. Total Revenue")

    # Apply sidebar filter (assumes you already have 'selected_years' list from the sidebar)
    df_filtered_years = df_mart[df_mart["year"].isin(selected_years)]

    df_nation = df_filtered_years.groupby("year").agg({
        "total_revenue": "sum",
        "median_household_income": "mean"
    }).reset_index()

    # Normalize both for comparison
    df_nation["income_index"] = df_nation["median_household_income"] / df_nation["median_household_income"].max()
    df_nation["revenue_index"] = df_nation["total_revenue"] / df_nation["total_revenue"].max()

    df_nation_long = df_nation.melt(id_vars="year", value_vars=["income_index", "revenue_index"],
                                    var_name="metric", value_name="value")

    line = alt.Chart(df_nation_long).mark_line(point=True).encode(
        x=alt.X("year:O", title="Year"),
        y=alt.Y("value:Q", title="Normalized Value (0–1)", axis=alt.Axis(format=".3f")),
        color=alt.Color("metric:N", title="Metric"),
        tooltip=[
            alt.Tooltip("year", title="Year"),
            alt.Tooltip("metric", title="Metric"),
            alt.Tooltip("value", title="Value", format=".3f")
        ]
    ).properties(
        title="Nationwide Trends: Normalized Median Income vs. Revenue",
        width=850,
        height=400
    )

    st.altair_chart(line, use_container_width=True)

   