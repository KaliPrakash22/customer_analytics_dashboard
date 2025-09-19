# frontend.py

import streamlit as st
import pandas as pd
import plotly.express as px
from backend import ingest_csv_data, get_all_data, get_business_insights, get_key_metrics
import os

# Set page configuration
st.set_page_config(layout="wide", page_title="Customer Analytics Dashboard")

# Custom CSS for styling
st.markdown("""
<style>
    .reportview-container {
        background: #f0f2f6;
    }
    .main {
        background-color: #ffffff;
        border-radius: 10px;
        box-shadow: 0 4px 8px 0 rgba(0,0,0,0.1);
        padding: 20px;
    }
    .st-emotion-cache-1f7l053 {
        border-radius: 10px;
        box-shadow: 0 2px 4px 0 rgba(0,0,0,0.05);
        padding: 10px;
        background-color: #f9f9f9;
    }
    .st-emotion-cache-1d374r {
        background-color: #4F8BF9;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

# --- Sidebar for Navigation ---
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Data Ingestion", "Analytics Dashboard"])

# --- Main Page Content ---

if page == "Data Ingestion":
    st.title("📂 Data Ingestion")
    st.write("Upload a CSV file to ingest new customer data into the database.")
    
    # File uploader widget
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
    
    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file)
            st.write("### Data Preview")
            st.dataframe(df.head())

            # Data ingestion button
            if st.button("Ingest Data"):
                with st.spinner('Ingesting data...'):
                    if ingest_csv_data(df):
                        st.success("Data successfully ingested!")
                    else:
                        st.error("Failed to ingest data. Check the backend logs.")
        except Exception as e:
            st.error(f"An error occurred while reading the CSV: {e}")

elif page == "Analytics Dashboard":
    st.title("📈 Customer Analytics Dashboard")
    
    # Cache all data for performance
    @st.cache_data(ttl=3600)
    def get_cached_data():
        df = get_all_data()
        # FIX: Convert the 'purchase_date' column to datetime here.
        if 'purchase_date' in df.columns:
            df['purchase_date'] = pd.to_datetime(df['purchase_date'])
        return df

    df_data = get_cached_data()
    
    if df_data.empty:
        st.warning("No data found in the database. Please go to the 'Data Ingestion' tab to upload a CSV file.")
    else:
        st.write("Explore key customer metrics and business insights.")

        # --- Key Metrics Section ---
        st.subheader("Key Business Metrics")
        try:
            # The get_key_metrics function will now receive a DataFrame with a proper datetime column
            metrics = get_key_metrics(df_data)
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Customer Lifetime Value (LTV)", metrics.get('LTV'))
            with col2:
                st.metric("Customer Acquisition Cost (CAC)", metrics.get('CAC'))
            with col3:
                st.metric("Churn Rate", metrics.get('Churn_Rate'))
        except KeyError:
            st.error("Missing required columns for metrics calculation (e.g., 'customer_id', 'purchase_date', 'purchase_amount').")
        
        # --- Business Insights Section (Dynamic Aggregations) ---
        st.subheader("Business Insights")
        
        # Using a selectbox for aggregation type
        agg_type = st.selectbox(
            "Select an Insight Type:",
            ["Total Revenue by Region", "Average Purchase by Segment", "Min/Max Purchase Amount", "Customer Count by Channel"]
        )

        if agg_type == "Total Revenue by Region":
            if 'region' in df_data.columns and 'purchase_amount' in df_data.columns:
                df_agg = df_data.groupby('region')['purchase_amount'].sum().reset_index()
                fig = px.bar(df_agg, x='region', y='purchase_amount', title="Total Revenue by Region")
                st.plotly_chart(fig)
            else:
                st.warning("Data is missing 'region' or 'purchase_amount' columns.")
        
        elif agg_type == "Average Purchase by Segment":
            if 'customer_segment' in df_data.columns and 'purchase_amount' in df_data.columns:
                df_agg = df_data.groupby('customer_segment')['purchase_amount'].mean().reset_index()
                fig = px.pie(df_agg, values='purchase_amount', names='customer_segment', title="Average Purchase by Customer Segment")
                st.plotly_chart(fig)
            else:
                st.warning("Data is missing 'customer_segment' or 'purchase_amount' columns.")

        elif agg_type == "Min/Max Purchase Amount":
            if 'purchase_amount' in df_data.columns:
                min_val = df_data['purchase_amount'].min()
                max_val = df_data['purchase_amount'].max()
                st.info(f"**Minimum Purchase:** ${min_val:,.2f}")
                st.info(f"**Maximum Purchase:** ${max_val:,.2f}")
            else:
                st.warning("Data is missing 'purchase_amount' column.")
        
        elif agg_type == "Customer Count by Channel":
            if 'acquisition_channel' in df_data.columns:
                df_agg = df_data.groupby('acquisition_channel')['customer_id'].nunique().reset_index()
                fig = px.bar(df_agg, x='acquisition_channel', y='customer_id', title="Customer Count by Acquisition Channel")
                st.plotly_chart(fig)
            else:
                st.warning("Data is missing 'acquisition_channel' column.")
        
        st.write("---")
        
        # --- Raw Data View ---
        st.subheader("Raw Customer Data")
        st.dataframe(df_data)