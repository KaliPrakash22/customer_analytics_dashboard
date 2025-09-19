# backend.py

import psycopg2
import pandas as pd
from io import StringIO
import os
import re

# Database connection details from environment variables or defaults
DB_NAME = os.environ.get("DB_NAME", "customer_db")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "KaliNew")
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")

def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        return conn
    except psycopg2.OperationalError as e:
        print(f"Error connecting to database: {e}")
        return None

def sanitize_column_name(col_name):
    """Sanitizes a string to be a valid PostgreSQL column name."""
    col_name = col_name.lower().replace(' ', '_')
    col_name = re.sub(r'[^a-z0-9_]', '', col_name)
    return f'"{col_name}"'

def infer_sql_type(col_series):
    """Infers the best PostgreSQL data type for a pandas Series."""
    if pd.api.types.is_numeric_dtype(col_series):
        if pd.api.types.is_integer_dtype(col_series):
            return "INTEGER"
        else:
            return "DECIMAL"
    elif pd.api.types.is_datetime64_any_dtype(col_series):
        return "TIMESTAMP"
    else:
        # Default to TEXT for strings and other types
        return "TEXT"

# --- CRUD Principles ---

# CREATE (Data Ingestion)
def ingest_csv_data(df, table_name="customer_data"):
    """
    Dynamically creates a table in the database from a DataFrame and
    ingests the data. It will overwrite any existing table with the same name.
    """
    conn = get_db_connection()
    if conn is None: return False

    try:
        cursor = conn.cursor()
        sanitized_table_name = sanitize_column_name(table_name).strip('"')

        # Drop table if it exists
        cursor.execute(f"DROP TABLE IF EXISTS {sanitized_table_name};")
        conn.commit()

        # Generate SQL for table creation
        schema_sql = []
        df.columns = [sanitize_column_name(col) for col in df.columns]
        for col, dtype in zip(df.columns, df.dtypes):
            sql_type = infer_sql_type(df[col])
            schema_sql.append(f"{col} {sql_type}")
        
        create_table_query = f"CREATE TABLE {sanitized_table_name} ({', '.join(schema_sql)});"
        cursor.execute(create_table_query)
        conn.commit()
        
        # Load data using COPY FROM
        buffer = StringIO()
        # Drop the " from column names for to_csv
        df.columns = [col.strip('"') for col in df.columns]
        df.to_csv(buffer, index=False, header=False, sep='\t')
        buffer.seek(0)
        
        cursor.copy_from(buffer, sanitized_table_name, sep='\t')
        conn.commit()
        
        print(f"Data successfully ingested into table '{sanitized_table_name}'.")
        return True
    except Exception as e:
        conn.rollback()
        print(f"Error during data ingestion: {e}")
        return False
    finally:
        if conn: cursor.close(); conn.close()

# READ (Data Retrieval & Analysis)
def get_all_data(table_name="customer_data"):
    """Fetches all data from the specified table."""
    conn = get_db_connection()
    if conn is None: return pd.DataFrame()
    
    try:
        sanitized_table_name = sanitize_column_name(table_name).strip('"')
        query = f"SELECT * FROM {sanitized_table_name};"
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()
    finally:
        if conn: conn.close()

def get_data_by_filters(query):
    """Executes a custom query to retrieve filtered data."""
    conn = get_db_connection()
    if conn is None: return pd.DataFrame()
    
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        print(f"Error fetching filtered data: {e}")
        return pd.DataFrame()
    finally:
        if conn: conn.close()

def get_key_metrics(df):
    """
    Calculates key business metrics from a DataFrame.
    Assumes columns: 'customer_id', 'purchase_date', 'purchase_amount'.
    """
    if df.empty:
        return {'LTV': 0, 'CAC': 0, 'Churn_Rate': 0}

    # Simulate CAC and Churn Rate based on available data
    # In a real-world scenario, this data would come from different tables.
    total_customers = df['customer_id'].nunique()
    total_revenue = df['purchase_amount'].sum()
    
    # Simplified LTV Calculation
    avg_purchase_value = df.groupby('customer_id')['purchase_amount'].mean().mean()
    customer_lifespan_days = (df['purchase_date'].max() - df['purchase_date'].min()).days
    avg_purchases_per_customer = df.shape[0] / total_customers
    ltv = (avg_purchase_value * avg_purchases_per_customer) * (customer_lifespan_days / 365)

    # Simplified CAC Calculation (assuming first purchase is acquisition)
    new_customers = df.groupby('customer_id')['purchase_date'].min().reset_index()
    cac = total_revenue / total_customers

    # Simplified Churn Rate (assuming no repeat purchase within a period implies churn)
    # This is a very basic proxy and should be refined with real data.
    latest_date = df['purchase_date'].max()
    churned_customers = df[df['purchase_date'] < (latest_date - pd.Timedelta(days=365))]['customer_id'].nunique()
    churn_rate = (churned_customers / total_customers) * 100

    return {
        'LTV': f"{ltv:,.2f}",
        'CAC': f"{cac:,.2f}",
        'Churn_Rate': f"{churn_rate:,.2f}%"
    }

def get_business_insights(df):
    """Provides key business insights using aggregation functions."""
    if df.empty:
        return {}
    
    insights = {}
    
    # COUNT
    total_customers = df['customer_id'].nunique()
    insights['Total_Customers'] = total_customers
    
    # SUM
    total_revenue = df['purchase_amount'].sum()
    insights['Total_Revenue'] = f"{total_revenue:,.2f}"
    
    # AVG
    avg_purchase_amount = df['purchase_amount'].mean()
    insights['Average_Purchase_Amount'] = f"{avg_purchase_amount:,.2f}"
    
    # MIN/MAX
    min_purchase = df['purchase_amount'].min()
    max_purchase = df['purchase_amount'].max()
    insights['Min_Purchase'] = f"{min_purchase:,.2f}"
    insights['Max_Purchase'] = f"{max_purchase:,.2f}"

    return insights