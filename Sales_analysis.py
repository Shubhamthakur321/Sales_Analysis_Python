import pandas as pd
import mysql.connector
import os

# List of CSV files and their corresponding table names
csv_files = [
    ('customers.csv', 'customers'),
    ('orders.csv', 'orders'),
    ('geolocation.csv', 'Geoloacation'),
    ('products.csv', 'products'),
    ('order_items.csv', 'item'),
    ('payments.csv', 'payments'),
    ('sellers.csv','seller')# Added payments.csv for specific handling
]

# Connect to the MySQL database
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='123456',
    database='product'
)
cursor = conn.cursor()

# Folder containing the CSV files
folder_path = r'C:\Users\shubham_kumar\Downloads\sales analysis'

def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'

for csv_file, table_name in csv_files:
    file_path = os.path.join(folder_path, csv_file)
    
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(file_path)
    
    # Replace NaN with None to handle SQL NULL
    df = df.where(pd.notnull(df), None)
    
    # Debugging: Check for NaN values
    print(f"Processing {csv_file}")
    print(f"NaN values before replacement:\n{df.isnull().sum()}\n")

    # Clean column names
    df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]

    # Generate the CREATE TABLE statement with appropriate data types
    columns = ', '.join([f'`{col}` {get_sql_type(df[col].dtype)}' for col in df.columns])
    create_table_query = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})'
    cursor.execute(create_table_query)

    # Insert DataFrame data into the MySQL table
    for _, row in df.iterrows():
        # Convert row to tuple and handle NaN/None explicitly
        values = tuple(None if pd.isna(x) else x for x in row)
        sql = f"INSERT INTO `{table_name}` ({', '.join(['`' + col + '`' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(row))})"
        cursor.execute(sql, values)

    # Commit the transaction for the current CSV file
    conn.commit()

# Close the connection
conn.close()


import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import mysql.connector

db= mysql.connector.connect(host = "localhost",
                           username = "root",
                           password = '123456',
                           database = 'product')

cur = db.cursor()

query = '''select distinct customer_city from customers'''
cur.execute(query)
data = cur.fetchall()

query = """SELECT COUNT(*) AS order_count FROM orders WHERE YEAR(order_approved_at) = 2017"""
cur.execute(query)
data1= cur.fetchall()

query = """select p.product_category as category, round(sum(payment_value),2) as amount from products p
join item it on it.product_id = p.product_id
join payments py on py.order_id = it.order_id
group by p.product_category"""

cur.execute(query)
data3 = cur.fetchall()
df3 = pd.DataFrame(data3,columns=['Categoy','Sales'])

df3['Categoy']=df3['Categoy'].str.upper()
df3

df3.isnull().sum()
df3 = df3.dropna()

plt.figure(figsize=(30,15))
df_filter = df3[df3['Sales'] > 10000000]
sns.barplot(data= df_filter,x='Categoy',y='Sales', hue='Categoy')
plt.xticks(rotation = -60)
plt.show()

query = '''select customer_state, count(customer_id) from customers
group by customer_state'''
cur.execute(query)
data4 = cur.fetchall()
df4 = pd.DataFrame(data4,columns= ['State','Customer_count'])
df4 = df4.sort_values('Customer_count',ascending=False)

plt.bar(df4["State"],df4["Customer_count"])
plt.xticks(rotation = 90)
plt.show()

query ='''select monthname(order_purchase_timestamp) as months, count(order_id) from product.orders
where year(order_purchase_timestamp) = 2018
group by monthname(order_purchase_timestamp) '''

cur.execute(query)
data5=cur.fetchall()
df5 = pd.DataFrame(data5,columns= ['State','Customer_count'])

o = ['January','February','March','April','May','June','July','August','September','October']
sns.barplot(data=df5,x=df5['State'],y=df5['Customer_count'], order=o)
plt.xticks(rotation = 90)
