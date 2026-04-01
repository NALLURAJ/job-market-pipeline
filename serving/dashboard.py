import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import snowflake.connector
import plotly.express as px
import pandas as pd
from config.config import SNOWFLAKE_CONFIG

st.set_page_config(page_title="Job Market Intelligence", layout="wide")
st.title("Job Market Intelligence Dashboard")

# Connect to Snowflake
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)

def run_query(query):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    return pd.DataFrame(data, columns=columns)

# --- Row 1: Key Metrics ---
col1, col2, col3, col4 = st.columns(4)

salary_by_role = run_query("SELECT * FROM ANALYTICS.MART_SALARY_BY_ROLE")
top_skills = run_query("SELECT * FROM ANALYTICS.MART_TOP_SKILLS")
by_location = run_query("SELECT * FROM ANALYTICS.MART_SALARY_BY_LOCATION")

total_jobs = salary_by_role["JOB_COUNT"].sum()
total_remote = by_location["REMOTE_JOBS"].sum()
avg_salary = salary_by_role["AVG_SALARY"].mean()
top_skill = top_skills.iloc[0]["SKILL_NAME"] if len(top_skills) > 0 else "N/A"

col1.metric("Total Jobs", f"{total_jobs:,}")
col2.metric("Remote Jobs", f"{total_remote:,}")
col3.metric("Avg Salary", f"${avg_salary:,.0f}")
col4.metric("Top Skill", top_skill)

st.divider()

# --- Row 2: Salary by Role + Top Skills ---
left, right = st.columns(2)

with left:
    st.subheader("Average Salary by Experience Level")
    fig1 = px.bar(
        salary_by_role,
        x="EXPERIENCE_LEVEL",
        y="AVG_SALARY",
        text="AVG_SALARY",
        color="EXPERIENCE_LEVEL",
    )
    fig1.update_traces(texttemplate="$%{text:,.0f}", textposition="outside")
    fig1.update_layout(showlegend=False, yaxis_title="Salary ($)")
    st.plotly_chart(fig1, use_container_width=True)

with right:
    st.subheader("Top 15 In-Demand Skills")
    top_15 = top_skills.head(15)
    fig2 = px.bar(
        top_15,
        x="DEMAND_COUNT",
        y="SKILL_NAME",
        orientation="h",
        color="SKILL_TYPE",
        text="DEMAND_COUNT",
    )
    fig2.update_layout(yaxis={"categoryorder": "total ascending"}, yaxis_title="")
    st.plotly_chart(fig2, use_container_width=True)

st.divider()

# --- Row 3: Location + Company ---
left2, right2 = st.columns(2)

with left2:
    st.subheader("Jobs by Location (Top 15)")
    top_locations = by_location.head(15)
    fig3 = px.bar(
        top_locations,
        x="JOB_COUNT",
        y="LOCATION",
        orientation="h",
        text="JOB_COUNT",
    )
    fig3.update_layout(yaxis={"categoryorder": "total ascending"}, yaxis_title="")
    st.plotly_chart(fig3, use_container_width=True)

with right2:
    st.subheader("Top Hiring Companies")
    by_company = run_query("SELECT * FROM ANALYTICS.MART_SALARY_BY_COMPANY ORDER BY TOTAL_OPENINGS DESC LIMIT 15")
    fig4 = px.bar(
        by_company,
        x="TOTAL_OPENINGS",
        y="COMPANY_NAME",
        orientation="h",
        text="TOTAL_OPENINGS",
    )
    fig4.update_layout(yaxis={"categoryorder": "total ascending"}, yaxis_title="")
    st.plotly_chart(fig4, use_container_width=True)

st.divider()

# --- Row 4: Salary Estimation Table ---
st.subheader("All Jobs with Estimated Salaries")
estimated = run_query("SELECT * FROM ANALYTICS.MART_ESTIMATED_SALARY ORDER BY ESTIMATED_SALARY DESC")
st.dataframe(
    estimated[["TITLE", "LOCATION", "EXPERIENCE_LEVEL", "SALARY_AVG", "ESTIMATED_SALARY", "SALARY_SOURCE"]],
    use_container_width=True,
    height=400,
)
