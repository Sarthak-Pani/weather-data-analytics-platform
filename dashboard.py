import streamlit as st
import mysql.connector
import pandas as pd

st.title("🌦 Weather Data Analytics Dashboard")

# Connect to MySQL
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="your_password",
    database="weather_analytics"
)

query = """
SELECT 
    dc.city_name,
    dd.full_date,
    fw.temperature,
    fw.humidity,
    fw.rainfall
FROM fact_weather fw
JOIN dim_city dc ON fw.city_id = dc.city_id
JOIN dim_date dd ON fw.date_id = dd.date_id
"""

df = pd.read_sql(query, connection)

connection.close()

st.subheader("Raw Weather Data")
st.dataframe(df)

st.sidebar.header("Filter Options")

selected_city = st.sidebar.selectbox(
    "Select City",
    df["city_name"].unique()
)

filtered_df = df[df["city_name"] == selected_city]

st.subheader("📊 Average Temperature per City")

avg_temp = filtered_df.groupby("city_name")["temperature"].mean().reset_index()

st.bar_chart(avg_temp.set_index("city_name"))

st.subheader("📈 Monthly Temperature Trend")

filtered_df["month"] = pd.to_datetime(filtered_df["full_date"]).dt.to_period("M")

monthly_avg = filtered_df.groupby("month")["temperature"].mean().reset_index()

monthly_avg["month"] = monthly_avg["month"].astype(str)

st.line_chart(monthly_avg.set_index("month"))