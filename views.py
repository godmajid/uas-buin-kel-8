# dashboard/views.py
from django.shortcuts import render
from django.http import JsonResponse
import psycopg2

def dashboard(request):
    # Koneksi ke database
    conn = psycopg2.connect(
        dbname='etldb',
        user='alfin',
        password='alfin123',
        host='localhost',
        port='5432'
    )
    cursor = conn.cursor()

    # Data untuk Sales by Region (Bar Chart)
    cursor.execute("""
        SELECT r.Region, SUM(f.Sales) as total_sales
        FROM fact_sales f
        JOIN dim_region r ON f.RegionID = r.RegionID
        GROUP BY r.Region
    """)
    region_data = cursor.fetchall()
    region_labels = [str(row[0]) for row in region_data]
    region_values = [float(row[1]) if row[1] else 0.0 for row in region_data]

    # Data untuk Sales Over Time (Line Chart)
    cursor.execute("""
        SELECT t.Year, t.Month, SUM(f.Sales) as total_sales
        FROM fact_sales f
        JOIN dim_date t ON f.OrderDate = t.DateID
        GROUP BY t.Year, t.Month
        ORDER BY t.Year, t.Month
    """)
    time_data = cursor.fetchall()
    time_labels = [f"{row[0]}-{row[1]:02d}" for row in time_data]
    time_values = [float(row[2]) if row[2] else 0.0 for row in time_data]

    # Data untuk Sales by Product Category (Pie Chart)
    cursor.execute("""
        SELECT p.Category, SUM(f.Sales) as total_sales
        FROM fact_sales f
        JOIN dim_product p ON f.ProductID = p.ProductID
        GROUP BY p.Category
    """)
    category_data = cursor.fetchall()
    category_labels = [str(row[0]) for row in category_data]
    category_values = [float(row[1]) if row[1] else 0.0 for row in category_data]

    # Data untuk Total Quantity by Customer Segment (Bar Chart)
    cursor.execute("""
        SELECT c.Segment, SUM(cb.Quantity) as total_quantity
        FROM fact_customer_behavior cb
        JOIN dim_customer c ON cb.CustomerID = c.CustomerID
        GROUP BY c.Segment
    """)
    customer_segment_data = cursor.fetchall()
    customer_segment_labels = [str(row[0]) for row in customer_segment_data]
    customer_segment_values = [float(row[1]) if row[1] else 0.0 for row in customer_segment_data]

    # Data untuk Profit Trend Over Time (Line Chart)
    cursor.execute("""
        SELECT t.Year, t.Month, SUM(cb.Profit) as total_profit
        FROM fact_customer_behavior cb
        JOIN dim_date t ON cb.DateID = t.DateID
        GROUP BY t.Year, t.Month
        ORDER BY t.Year, t.Month
    """)
    profit_time_data = cursor.fetchall()
    profit_time_labels = [f"{row[0]}-{row[1]:02d}" for row in profit_time_data]
    profit_time_values = [float(row[2]) if row[2] else 0.0 for row in profit_time_data]

    # Data untuk Sales by Ship Mode (Bar Chart)
    cursor.execute("""
        SELECT s.ShipMode, SUM(fs.Sales) as total_sales
        FROM fact_shipping fs
        JOIN dim_shipmode s ON fs.ShipModeID = s.ShipModeID
        GROUP BY s.ShipMode
    """)
    ship_mode_data = cursor.fetchall()
    ship_mode_labels = [str(row[0]) for row in ship_mode_data]
    ship_mode_values = [float(row[1]) if row[1] else 0.0 for row in ship_mode_data]


    # Data untuk Customer Segmentation by Purchase Value (Bar Chart)
    cursor.execute("""
        SELECT c.Segment, SUM(cb.Sales) as total_purchase_value
        FROM fact_customer_behavior cb
        JOIN dim_customer c ON cb.CustomerID = c.CustomerID
        GROUP BY c.Segment
    """)
    purchase_value_data = cursor.fetchall()
    purchase_value_labels = [str(row[0]) for row in purchase_value_data]
    purchase_value_values = [float(row[1]) if row[1] else 0.0 for row in purchase_value_data]

   
    # Data untuk Product Performance for Stock Recommendation (Bar Chart)
    cursor.execute("""
        SELECT p.ProductName, SUM(f.Sales) as total_sales
        FROM fact_sales f
        JOIN dim_product p ON f.ProductID = p.ProductID
        GROUP BY p.ProductName
        ORDER BY total_sales DESC
        LIMIT 10
    """)
    product_data = cursor.fetchall()
    product_labels = [str(row[0]) for row in product_data]
    product_values = [float(row[1]) if row[1] else 0.0 for row in product_data]

    # Data untuk Average Delivery Time by Ship Mode (Bar Chart)
    cursor.execute("""
        SELECT s.ShipMode, AVG(EXTRACT(EPOCH FROM AGE(fs.ShipDate, fs.OrderDate)) / 86400) as avg_delivery_time
        FROM fact_shipping fs
        JOIN dim_shipmode s ON fs.ShipModeID = s.ShipModeID
        WHERE fs.ShipDate IS NOT NULL AND fs.OrderDate IS NOT NULL
        GROUP BY s.ShipMode
    """)
    delivery_time_data = cursor.fetchall()
    delivery_time_labels = [str(row[0]) for row in delivery_time_data]
    delivery_time_values = [float(row[1]) if row[1] else 0.0 for row in delivery_time_data]

    # Data untuk Average Profit by Shipping Method (Bar Chart - Tambahan)
    cursor.execute("""
        SELECT s.ShipMode, AVG(fs.Profit) as avg_profit
        FROM fact_shipping fs
        JOIN dim_shipmode s ON fs.ShipModeID = s.ShipModeID
        WHERE fs.Profit IS NOT NULL
        GROUP BY s.ShipMode
    """)
    profit_by_shipmode_data = cursor.fetchall()
    profit_by_shipmode_labels = [str(row[0]) for row in profit_by_shipmode_data]
    profit_by_shipmode_values = [float(row[1]) if row[1] else 0.0 for row in profit_by_shipmode_data]

    # Tutup koneksi
    cursor.close()
    conn.close()

    return render(request, 'dashboard/index.html', {
        'region_labels': region_labels,
        'region_values': region_values,
        'time_labels': time_labels,
        'time_values': time_values,
        'category_labels': category_labels,
        'category_values': category_values,
        'customer_segment_labels': customer_segment_labels,
        'customer_segment_values': customer_segment_values,
        'profit_time_labels': profit_time_labels,
        'profit_time_values': profit_time_values,
        'ship_mode_labels': ship_mode_labels,
        'ship_mode_values': ship_mode_values,
        'purchase_value_labels': purchase_value_labels,
        'purchase_value_values': purchase_value_values,
        'product_labels': product_labels,
        'product_values': product_values,
        'delivery_time_labels': delivery_time_labels,
        'delivery_time_values': delivery_time_values,
        'profit_by_shipmode_labels': profit_by_shipmode_labels,
        'profit_by_shipmode_values': profit_by_shipmode_values,
    })