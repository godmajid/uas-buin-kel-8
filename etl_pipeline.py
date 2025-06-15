from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2

# --- Extract ---
def extract():
    try:
        df = pd.read_csv('/home/dell/airflow/datasets/superstore.csv', encoding='latin-1')
        df.to_csv('/tmp/superstore_extracted.csv', index=False, encoding='utf-8')
    except UnicodeDecodeError as e:
        print(f"Error decoding file: {e}")
        df = pd.read_csv('/home/dell/airflow/datasets/superstore.csv', encoding='cp1252')
        df.to_csv('/tmp/superstore_extracted.csv', index=False, encoding='utf-8')

# --- Transform ---
def transform():
    df = pd.read_csv('/tmp/superstore_extracted.csv')
    
    # Konversi tipe data
    df['Order Date'] = pd.to_datetime(df['Order Date'], format='%m/%d/%Y', errors='coerce')
    df['Ship Date'] = pd.to_datetime(df['Ship Date'], format='%m/%d/%Y', errors='coerce')
    df['Sales'] = pd.to_numeric(df['Sales'], errors='coerce')
    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
    df['Discount'] = pd.to_numeric(df['Discount'], errors='coerce')
    df['Profit'] = pd.to_numeric(df['Profit'], errors='coerce')
    
    # Debugging: Periksa nilai yang tidak valid di Order Date dan Region
    invalid_dates = df[df['Order Date'].isna()]
    if not invalid_dates.empty:
        print("Baris dengan Order Date tidak valid:")
        print(invalid_dates[['Order ID', 'Order Date']])
    
    invalid_regions = df[df['Region'].isna()]
    if not invalid_regions.empty:
        print("Baris dengan Region tidak valid:")
        print(invalid_regions[['Order ID', 'Region']])
    
    # Hapus baris dengan Order Date, Ship Date, atau Region yang tidak valid
    df = df.dropna(subset=['Order Date', 'Ship Date', 'Region'])
    
    # Ganti nilai NaN di Region dengan 'Unknown' dan pastikan tipe data konsisten
    df['Region'] = df['Region'].fillna('Unknown')
    df['RegionID'] = df['Region'].astype(str)
    df['ShipModeID'] = df['Ship Mode'].astype(str)
    
    df.to_csv('/tmp/superstore_transformed.csv', index=False)

# --- Load ---
def load():
    df = pd.read_csv('/tmp/superstore_transformed.csv')
    
    # Konversi kolom tanggal kembali ke datetime untuk memastikan tipe yang benar
    df['Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce')
    df['Ship Date'] = pd.to_datetime(df['Ship Date'], errors='coerce')

    conn = psycopg2.connect(
        dbname='etldb',
        user='alfin',
        password='alfin123',
        host='localhost',
        port='5432'
    )
    cur = conn.cursor()

    # Buat tabel dimensi dan fakta jika belum ada
    create_dim_customer = """
    CREATE TABLE IF NOT EXISTS dim_customer (
        CustomerID VARCHAR PRIMARY KEY,
        CustomerName VARCHAR,
        Segment VARCHAR,
        State VARCHAR,
        Region VARCHAR
    );
    """
    create_dim_product = """
    CREATE TABLE IF NOT EXISTS dim_product (
        ProductID VARCHAR PRIMARY KEY,
        ProductName VARCHAR,
        Category VARCHAR,
        SubCategory VARCHAR
    );
    """
    create_dim_date = """
    CREATE TABLE IF NOT EXISTS dim_date (
        DateID DATE PRIMARY KEY,
        Year INT,
        Month INT,
        Quarter INT,
        Weekday VARCHAR
    );
    """
    create_dim_region = """
    CREATE TABLE IF NOT EXISTS dim_region (
        RegionID VARCHAR PRIMARY KEY,
        Country VARCHAR,
        Region VARCHAR,
        State VARCHAR,
        City VARCHAR
    );
    """
    create_dim_shipmode = """
    CREATE TABLE IF NOT EXISTS dim_shipmode (
        ShipModeID VARCHAR PRIMARY KEY,
        ShipMode VARCHAR
    );
    """
    create_fact_customer_behavior = """
    CREATE TABLE IF NOT EXISTS fact_customer_behavior (
        OrderID VARCHAR PRIMARY KEY,
        CustomerID VARCHAR,
        ProductID VARCHAR,
        DateID DATE,
        Sales DECIMAL,
        Quantity INT,
        Discount DECIMAL,
        Profit DECIMAL,
        FOREIGN KEY (CustomerID) REFERENCES dim_customer(CustomerID),
        FOREIGN KEY (ProductID) REFERENCES dim_product(ProductID),
        FOREIGN KEY (DateID) REFERENCES dim_date(DateID)
    );
    """
    create_fact_sales = """
    CREATE TABLE IF NOT EXISTS fact_sales (
        OrderID VARCHAR PRIMARY KEY,
        CustomerID VARCHAR,
        ProductID VARCHAR,
        RegionID VARCHAR,
        OrderDate DATE,
        Sales DECIMAL,
        Quantity INT,
        Discount DECIMAL,
        Profit DECIMAL,
        FOREIGN KEY (CustomerID) REFERENCES dim_customer(CustomerID),
        FOREIGN KEY (ProductID) REFERENCES dim_product(ProductID),
        FOREIGN KEY (RegionID) REFERENCES dim_region(RegionID)
    );
    """
    create_fact_shipping = """
    CREATE TABLE IF NOT EXISTS fact_shipping (
        OrderID VARCHAR PRIMARY KEY,
        CustomerID VARCHAR,
        ProductID VARCHAR,
        ShipModeID VARCHAR,
        OrderDate DATE,
        ShipDate DATE,
        Sales DECIMAL,
        Discount DECIMAL,
        Profit DECIMAL,
        FOREIGN KEY (CustomerID) REFERENCES dim_customer(CustomerID),
        FOREIGN KEY (ProductID) REFERENCES dim_product(ProductID),
        FOREIGN KEY (ShipModeID) REFERENCES dim_shipmode(ShipModeID)
    );
    """

    for query in [create_dim_customer, create_dim_product, create_dim_date, create_dim_region,
                  create_dim_shipmode, create_fact_customer_behavior, create_fact_sales, create_fact_shipping]:
        cur.execute(query)
    conn.commit()

    # Sisipkan data ke tabel dimensi
    dim_customer_data = df[['Customer ID', 'Customer Name', 'Segment', 'State', 'Region']].drop_duplicates()
    for _, row in dim_customer_data.iterrows():
        cur.execute("""
            INSERT INTO dim_customer (CustomerID, CustomerName, Segment, State, Region)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (CustomerID) DO NOTHING;
        """, (str(row['Customer ID']), str(row['Customer Name']), str(row['Segment']), str(row['State']), str(row['Region'])))

   
    dim_region_data = df[['RegionID', 'Country', 'Region', 'State', 'City']].drop_duplicates()
    for _, row in dim_region_data.iterrows():
        cur.execute("""
            INSERT INTO dim_region (RegionID, Country, Region, State, City)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (RegionID) DO NOTHING;
        """, (str(row['RegionID']), str(row['Country']), str(row['Region']), str(row['State']), str(row['City'])))

    dim_product_data = df[['Product ID', 'Product Name', 'Category', 'Sub-Category']].drop_duplicates()
    for _, row in dim_product_data.iterrows():
        cur.execute("""
            INSERT INTO dim_product (ProductID, ProductName, Category, SubCategory)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (ProductID) DO NOTHING;
        """, (str(row['Product ID']), str(row['Product Name']), str(row['Category']), str(row['Sub-Category'])))

    # Sisipkan data ke dim_date
    dim_date_data = df[['Order Date']].drop_duplicates()
    if not dim_date_data.empty:
        dim_date_data['Order Date'] = pd.to_datetime(dim_date_data['Order Date'], errors='coerce')
        dim_date_data = dim_date_data.dropna(subset=['Order Date'])
        if not dim_date_data.empty:
            dim_date_data['Year'] = dim_date_data['Order Date'].dt.year
            dim_date_data['Month'] = dim_date_data['Order Date'].dt.month
            dim_date_data['Quarter'] = dim_date_data['Order Date'].dt.quarter
            dim_date_data['Weekday'] = dim_date_data['Order Date'].dt.day_name()
            for _, row in dim_date_data.iterrows():
                cur.execute("""
                    INSERT INTO dim_date (DateID, Year, Month, Quarter, Weekday)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (DateID) DO NOTHING;
                """, (row['Order Date'].to_pydatetime().date(), int(row['Year']), int(row['Month']), int(row['Quarter']), str(row['Weekday'])))

    dim_shipmode_data = df[['Ship Mode']].drop_duplicates()
    for _, row in dim_shipmode_data.iterrows():
        cur.execute("""
            INSERT INTO dim_shipmode (ShipModeID, ShipMode)
            VALUES (%s, %s)
            ON CONFLICT (ShipModeID) DO NOTHING;
        """, (str(row['Ship Mode']), str(row['Ship Mode'])))

    # Sisipkan data ke tabel fakta
    for _, row in df.iterrows():
        # Konversi tanggal ke format yang kompatibel dengan PostgreSQL
        order_date = row['Order Date'].to_pydatetime().date() if pd.notna(row['Order Date']) else None
        ship_date = row['Ship Date'].to_pydatetime().date() if pd.notna(row['Ship Date']) else None
        
        cur.execute("""
            INSERT INTO fact_customer_behavior (OrderID, CustomerID, ProductID, DateID, Sales, Quantity, Discount, Profit)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (OrderID) DO NOTHING;
        """, (str(row['Order ID']), str(row['Customer ID']), str(row['Product ID']), order_date, float(row['Sales']) if pd.notna(row['Sales']) else None, int(row['Quantity']) if pd.notna(row['Quantity']) else None, float(row['Discount']) if pd.notna(row['Discount']) else None, float(row['Profit']) if pd.notna(row['Profit']) else None))

        cur.execute("""
            INSERT INTO fact_sales (OrderID, CustomerID, ProductID, RegionID, OrderDate, Sales, Quantity, Discount, Profit)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (OrderID) DO NOTHING;
        """, (str(row['Order ID']), str(row['Customer ID']), str(row['Product ID']), str(row['RegionID']), order_date, float(row['Sales']) if pd.notna(row['Sales']) else None, int(row['Quantity']) if pd.notna(row['Quantity']) else None, float(row['Discount']) if pd.notna(row['Discount']) else None, float(row['Profit']) if pd.notna(row['Profit']) else None))

        cur.execute("""
            INSERT INTO fact_shipping (OrderID, CustomerID, ProductID, ShipModeID, OrderDate, ShipDate, Sales, Discount, Profit)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (OrderID) DO NOTHING;
        """, (str(row['Order ID']), str(row['Customer ID']), str(row['Product ID']), str(row['ShipModeID']), order_date, ship_date, float(row['Sales']) if pd.notna(row['Sales']) else None, float(row['Discount']) if pd.notna(row['Discount']) else None, float(row['Profit']) if pd.notna(row['Profit']) else None))

    conn.commit()
    cur.close()
    conn.close()

# --- DAG Definition ---
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='etl_superstore_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['ETL']
) as dag:

    t1 = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    t2 = PythonOperator(
        task_id='transform',
        python_callable=transform
    )

    t3 = PythonOperator(
        task_id='load',
        python_callable=load
    )

    t1 >> t2 >> t3