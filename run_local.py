import pandas as pd
import sys
import os

sys.path.insert(0, os.path.abspath("src"))
from data_transformer import clean_and_transform_sales_data

print("1. Loading sample data...")
df_raw = pd.read_csv("data/sample_sales_data.csv")

print("2. Running data cleaning and transformation...")
df_clean, df_error = clean_and_transform_sales_data(df_raw)

print("\n=== PIPELINE RESULT ===")
print(f"Total input rows: {len(df_raw)}")
print(f"Clean rows: {len(df_clean)}")
print(f"Error rows: {len(df_error)}")

print("\n=== BUSINESS METRICS ===")

# Only paid transactions are counted for revenue
paid_df = df_clean[df_clean["payment_status"].str.lower() == "paid"].copy()

# Daily revenue
daily_revenue = paid_df.groupby("order_date")["line_revenue"].sum()
print("\n1. Daily Revenue")
print(daily_revenue.to_string())

# Top products
top_products = (
    paid_df.groupby("product_id")["quantity"]
    .sum()
    .sort_values(ascending=False)
    .head(3)
)
print("\n2. Top Products")
print(top_products.to_string())

# Payment success rate
payment_success_rate = (df_clean["payment_status"].str.lower() == "paid").mean() * 100
print(f"\n3. Payment Success Rate: {payment_success_rate:.2f}%")

# Orders per customer
orders_per_customer = (
    df_clean.groupby("customer_id")["order_id"]
    .nunique()
)

print("\n4. Orders per Customer")
print(orders_per_customer.to_string())