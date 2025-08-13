import pandas as pd
import time
import logging
import sqlite3
from sqlalchemy import create_engine
from ingestion_db import ingest_db

# Setup logging
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

def create_vendor_summary(conn):
    """Merge different tables to create vendor sales summary"""
    vendor_sales_summary = pd.read_sql_query("""
    WITH FreightSummary AS (
        SELECT VendorNumber, SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),
    PurchaseSummary AS (
        SELECT p.VendorNumber, p.VendorName, p.Brand, p.Description,
               p.PurchasePrice, pp.Volume, pp.Price AS ActualPrice,
               SUM(p.Quantity) AS TotalPurchaseQuantity,
               SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp ON p.Brand = pp.Brand
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description,
                 p.PurchasePrice, pp.Volume, pp.Price
    ),
    SalesSummary AS (
        SELECT VendorNo, Brand,
               SUM(SalesDollars) AS TotalSalesDollars,
               SUM(SalesPrice) AS TotalSalesPrice,
               SUM(SalesQuantity) AS TotalSalesQuantity,
               SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )
    SELECT ps.VendorNumber, ps.VendorName, ps.Brand, ps.ActualPrice,
           ps.Description, ps.PurchasePrice, ps.Volume,
           ps.TotalPurchaseQuantity, ps.TotalPurchaseDollars,
           ss.TotalSalesQuantity, ss.TotalSalesDollars, ss.TotalSalesPrice,
           ss.TotalExciseTax, fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC
    """, conn)

    return vendor_sales_summary


def clean_data(vendor_sales_summary):
    """this function will clean the data"""

    # changing datatype to float
    vendor_sales_summary['Volume'] = vendor_sales_summary['Volume'].astype('float')

    # convert object dtypes to numeric/string where possible (prevents warning)
    vendor_sales_summary = vendor_sales_summary.infer_objects(copy=False)

    # now safely fill missing values with 0 (no FutureWarning)
    vendor_sales_summary = vendor_sales_summary.fillna(0)

    # removing spaces from categorical columns
    vendor_sales_summary['VendorName'] = vendor_sales_summary['VendorName'].str.strip()
    vendor_sales_summary['Description'] = vendor_sales_summary['Description'].str.strip()

    # creating new columns for better analysis
    vendor_sales_summary['GrossProfit'] = (
        vendor_sales_summary['TotalSalesDollars'] - vendor_sales_summary['TotalPurchaseDollars']
    )
    vendor_sales_summary['ProfitMargin'] = (
        vendor_sales_summary['GrossProfit'] / vendor_sales_summary['TotalSalesDollars'] * 100
    )
    vendor_sales_summary['StockTurnover'] = (
        vendor_sales_summary['TotalSalesQuantity'] / vendor_sales_summary['TotalPurchaseQuantity']
    )
    vendor_sales_summary['SalesToPurchaseRatio'] = (
        vendor_sales_summary['TotalSalesDollars'] / vendor_sales_summary['TotalPurchaseDollars']
    )

    return vendor_sales_summary


if __name__ == '__main__':
    # Create database connection
    conn = sqlite3.connect('Inventory.db')

    logging.info('Creating Vendor Summary Table...')
    vendor_sales_summary = create_vendor_summary(conn)
    logging.info(vendor_sales_summary.head())

    logging.info('Cleaning Data...')
    vendor_sales_summary = clean_data(vendor_sales_summary)
    logging.info(vendor_sales_summary.head())

    logging.info('Ingesting data...')
    ingest_db(vendor_sales_summary, 'vendor_sales_summary', conn)
    logging.info('Completed')
