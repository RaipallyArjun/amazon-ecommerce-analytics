import pandas as pd
from sqlalchemy import create_engine
import os
import logging

# -----------------------
# Configuration
# -----------------------

DATA_PATH = "data/raw/amazon_sales_data 2025.csv"
PROCESSED_PATH = "data/processed/amazon_sales_cleaned.csv"
DB_PATH = "sqlite:///amazon_ecommerce.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -----------------------
# ETL Function
# -----------------------

def run_etl():
    logging.info("ETL Process Started")

    # 1. Load Data
    df = pd.read_csv(DATA_PATH)
    logging.info(f"Initial dataset shape: {df.shape}")

    # Standardize column names
    df.columns = df.columns.str.strip().str.replace(" ", "_")

    # 2. Data Cleaning
    df["Date"] = pd.to_datetime(df["Date"], format="%d-%m-%y", errors="coerce")

    df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")

    df = df[df["Status"] != "Cancelled"]
    df = df[(df["Quantity"] > 0) & (df["Price"] > 0)]
    df = df.dropna(subset=["Date"])

    # 3. Feature Engineering
    df["Total_Sales"] = df["Price"] * df["Quantity"]

    df["OrderYear"] = df["Date"].dt.year
    df["OrderMonth"] = df["Date"].dt.month
    df["OrderYearMonth"] = df["Date"].dt.to_period("M").astype(str)

    df["CohortMonth"] = (
        df.groupby("Customer_Name")["Date"]
        .transform("min")
        .dt.to_period("M")
        .astype(str)
    )

    # Profit Model (60% cost assumption)
    df["Cost_Per_Unit"] = df["Price"] * 0.6
    df["Total_Cost"] = df["Cost_Per_Unit"] * df["Quantity"]
    df["Profit"] = df["Total_Sales"] - df["Total_Cost"]

    logging.info(f"Cleaned dataset shape: {df.shape}")

    # 4. Save Processed CSV
    os.makedirs("data/processed", exist_ok=True)
    df.to_csv(PROCESSED_PATH, index=False)
    logging.info("Processed CSV saved")

    # 5. Save to SQLite
    engine = create_engine(DB_PATH)
    df.to_sql("sales_data", engine, if_exists="replace", index=False)
    logging.info("Data loaded into SQLite")

    logging.info("ETL Completed Successfully")


# -----------------------
# RFM Analysis
# -----------------------

def run_rfm():
    logging.info("Running RFM Analysis")

    engine = create_engine(DB_PATH)

    rfm_query = """
    WITH CustomerRFM AS (
        SELECT
            Customer_Name,
            MAX(Date) AS LastPurchaseDate,
            COUNT(DISTINCT Order_ID) AS Frequency,
            SUM(Total_Sales) AS Monetary
        FROM sales_data
        GROUP BY Customer_Name
    ),

    RFM_Base AS (
        SELECT
            Customer_Name,
            (JULIANDAY('2025-04-01') - JULIANDAY(LastPurchaseDate)) AS Recency,
            Frequency,
            Monetary
        FROM CustomerRFM
    ),

    RFM_Score AS (
        SELECT *,
            NTILE(5) OVER (ORDER BY Recency DESC) AS R_Score,
            NTILE(5) OVER (ORDER BY Frequency ASC) AS F_Score,
            NTILE(5) OVER (ORDER BY Monetary ASC) AS M_Score
        FROM RFM_Base
    )

    SELECT *,
        (R_Score + F_Score + M_Score) AS RFM_Total_Score
    FROM RFM_Score
    ORDER BY RFM_Total_Score DESC;
    """

    df_rfm = pd.read_sql(rfm_query, engine)
    df_rfm.to_csv("data/processed/rfm_results.csv", index=False)

    logging.info("RFM Results saved")


# -----------------------
# Monthly Revenue
# -----------------------

def run_monthly_summary():
    logging.info("Generating Monthly Summary")

    engine = create_engine(DB_PATH)

    monthly_query = """
    SELECT
        strftime('%Y-%m', Date) AS SalesMonth,
        SUM(Total_Sales) AS MonthlyRevenue,
        COUNT(DISTINCT Order_ID) AS TotalOrders,
        COUNT(DISTINCT Customer_Name) AS UniqueCustomers
    FROM sales_data
    GROUP BY SalesMonth
    ORDER BY SalesMonth;
    """

    df_monthly = pd.read_sql(monthly_query, engine)
    df_monthly.to_csv("data/processed/monthly_summary.csv", index=False)

    logging.info("Monthly Summary saved")


# -----------------------
# Run Everything
# -----------------------

if __name__ == "__main__":
    run_etl()
    run_rfm()
    run_monthly_summary()