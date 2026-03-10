import streamlit as st
import redis
import pandas as pd
import time

r = redis.Redis(host="redis", port=6379, decode_responses=True)

st.set_page_config(page_title="IoT Real-Time Leaderboard", layout="wide")

st.title("📊 Real-Time Order Dashboard")

col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("Global Statistics")
    total_users_metric = st.empty()
    highest_order_metric = st.empty()

with col2:
    st.subheader("Top 10 Users by Max Order")
    leaderboard_table = st.empty()

while True:
    
    top_users_raw = r.zrevrange("orders:max_amount", 0, 9, withscores=True)
    
    if top_users_raw:
        df = pd.DataFrame(top_users_raw, columns=["User ID", "Max Amount"])
        
        total_users_metric.metric("Active Users Tracked", r.zcard("orders:max_amount"))
        highest_order_metric.metric("Highest Single Order", f"${df['Max Amount'].max():,.2f}")
        
        leaderboard_table.table(df)
    else:
        leaderboard_table.info("Waiting for data from Spark...")

    time.sleep(1)