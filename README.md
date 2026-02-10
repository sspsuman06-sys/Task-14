import pandas as pd
import sqlite3
import os

# Create folders
os.makedirs("raw", exist_ok=True)
os.makedirs("processed", exist_ok=True)
os.makedirs("output", exist_ok=True)

# =======================
# EXTRACT
# =======================
retail = pd.read_csv("raw/retail_sales_dataset.csv")
ecommerce = pd.read_csv("raw/ecommerce_dataset_updated.csv")
churn = pd.read_csv("raw/customer_churn.csv")

print("Extracted Data Shapes:")
print(retail.shape, ecommerce.shape, churn.shape)

# =======================
# TRANSFORM
# =======================

def clean_data(df):
    df = df.drop_duplicates()
    df = df.fillna(method="ffill")
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    return df

retail_cleaned = clean_data(retail)
ecommerce_cleaned = clean_data(ecommerce)
churn_cleaned = clean_data(churn)

# Save processed files
retail_cleaned.to_csv("processed/retail_cleaned.csv", index=False)
ecommerce_cleaned.to_csv("processed/ecommerce_cleaned.csv", index=False)
churn_cleaned.to_csv("processed/churn_cleaned.csv", index=False)

# =======================
# DERIVED COLUMNS
# =======================
if "sales" in retail_cleaned.columns and "profit" in retail_cleaned.columns:
    retail_cleaned["profit_margin"] = retail_cleaned["profit"] / retail_cleaned["sales"]

# =======================
# SPLIT TABLES
# =======================
customers = churn_cleaned.iloc[:, :5]
orders = retail_cleaned.iloc[:, :6]
products = ecommerce_cleaned.iloc[:, :6]

customers.to_csv("output/customers.csv", index=False)
orders.to_csv("output/orders.csv", index=False)
products.to_csv("output/products.csv", index=False)

# =======================
# LOAD (SQLite)
# =======================
conn = sqlite3.connect("database.sqlite")

customers.to_sql("customers", conn, if_exists="replace", index=False)
orders.to_sql("orders", conn, if_exists="replace", index=False)
products.to_sql("products", conn, if_exists="replace", index=False)

conn.close()

print("ETL Pipeline Executed Successfully ✅")
