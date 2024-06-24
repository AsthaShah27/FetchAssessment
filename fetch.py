import sqlite3
import pandas as pd
import json

# Create a new SQLite database (or connect to an existing one)
conn = sqlite3.connect('data_warehouse.db')
cursor = conn.cursor()

cursor.execute('DROP TABLE IF EXISTS Users')
cursor.execute('DROP TABLE IF EXISTS Brands')
cursor.execute('DROP TABLE IF EXISTS Receipts')
cursor.execute('DROP TABLE IF EXISTS Receipt_Items')

# Create Users table
cursor.execute('''
CREATE TABLE IF NOT EXISTS Users (
    user_id TEXT PRIMARY KEY,
    active BOOLEAN,
    created_date TEXT,
    last_login TEXT,
    role TEXT,
    sign_up_source TEXT,
    state TEXT
)
''')

# Create Receipts table
cursor.execute('''
CREATE TABLE IF NOT EXISTS Receipts (
    receipt_id TEXT PRIMARY KEY,
    user_id TEXT,
    date TEXT,
    date_scanned TEXT,
    total_spent REAL,
    rewardsReceiptStatus TEXT,
    FOREIGN KEY (user_id) REFERENCES Users(user_id)
)
''')

# Create Brands table
cursor.execute('''
CREATE TABLE IF NOT EXISTS Brands (
    brand_id TEXT PRIMARY KEY,
    name TEXT,
    category TEXT,
    category_code TEXT,
    barcode TEXT,
    brand_code TEXT,
    top_brand INTEGER
)
''')

# Create Receipt_Items table
cursor.execute('''
CREATE TABLE IF NOT EXISTS Receipt_Items (
    receipt_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
    receipt_id TEXT,
    barcode TEXT,
    item_price INTTEGER,
    quantity INTEGER,
    rewardsReceiptStatus TEXT,
    FOREIGN KEY (receipt_id) REFERENCES Receipts(receipt_id)
    )''')

conn.commit()

# Load JSON data
users_df = pd.read_json('users.json',lines=True)
print("\nUsers Table:", users_df.head())

brands_df = pd.read_json('brands.json', lines=True)
print("\nBrands Table:", brands_df.head())

receipts_df = pd.read_json('receipts.json',lines=True)



def transform_user_data(users_df):
    users_df['_id'] = users_df['_id'].apply(lambda x: x['$oid'] if isinstance(x, dict) else str(x))
    # Handle dates properly
    users_df['createdDate'] = pd.to_datetime(users_df['createdDate'].apply(lambda x: x['$date'] if isinstance(x, dict) else pd.NaT), unit='ms')
    users_df['lastLogin'] = pd.to_datetime(users_df['lastLogin'].apply(lambda x: x['$date'] if isinstance(x, dict) else pd.NaT), unit='ms')
    # Convert BOOLEAN to INTEGER
    users_df['active'] = users_df['active'].apply(lambda x: 1 if x else 0)
    # Convert dates to string for SQLite
    users_df['createdDate'] = users_df['createdDate'].astype(str)
    users_df['lastLogin'] = users_df['lastLogin'].astype(str)
    return users_df

def transform_brand_data(brands_df):
    brands_df['_id'] = brands_df['_id'].apply(lambda x: x['$oid'] if isinstance(x, dict) else str(x))
    brands_df['topBrand'] = brands_df['topBrand'].apply(lambda x: 1 if x else 0)
    return brands_df

def transform_receipts_data(receipts_df):
    receipts_df['_id'] = receipts_df['_id'].apply(lambda x: x['$oid'] if isinstance(x, dict) else str(x))
    receipts_df['userId'] = receipts_df['userId'].apply(lambda x: x['$oid'] if isinstance(x, dict) else str(x))
    receipts_df['createDate'] = pd.to_datetime(receipts_df['createDate'].apply(lambda x: x['$date'] if isinstance(x, dict) else pd.NaT), unit='ms')
    receipts_df['dateScanned'] = pd.to_datetime(receipts_df['dateScanned'].apply(lambda x: x['$date'] if isinstance(x, dict) else pd.NaT), unit='ms')
    receipts_df['createDate'] = receipts_df['createDate'].astype(str)
    receipts_df['dateScanned'] = receipts_df['dateScanned'].astype(str)
    receipts_df['rewardsReceiptItemList'] = receipts_df['rewardsReceiptItemList'].apply(lambda x: x if isinstance(x, list) else [])
    receipts_df['totalSpent'] = receipts_df['totalSpent'].apply(lambda x: float(x) if pd.notnull(x) else 0.0)
    return receipts_df

users_df = transform_user_data(users_df)
brands_df = transform_brand_data(brands_df)
receipts_df = transform_receipts_data(receipts_df)

users_df = users_df.drop_duplicates(subset=['_id'])
print("\nModified Users Table - sample data",users_df.head())

brands_df = brands_df.drop_duplicates(subset=['_id'])
print("\nModified Brands Table - sample data", brands_df.head())

receipts_df = receipts_df.drop_duplicates(subset=['_id'])


# Insert data into Users table
for _, row in users_df.iterrows():
    cursor.execute('''
    INSERT INTO Users (user_id, active, created_date, last_login, role, sign_up_source, state) 
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (row['_id'], row['active'], row['createdDate'], row['lastLogin'], row['role'], row['signUpSource'], row['state']))


for index, row in brands_df.iterrows():
    try:
        cursor.execute('''
        INSERT OR REPLACE INTO Brands (brand_id, name, category, category_code, barcode, brand_code, top_brand) 
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (row['_id'], row['name'], row['category'], row['categoryCode'], row['barcode'], row['brandCode'], row['topBrand']))
    except Exception as e:
        print(f"Error inserting row {index} into Brands table: {e}, Data: {row}")

# Insert data into Receipts and Receipt_Items table
for index, row in receipts_df.iterrows():
    try:
        cursor.execute('''
        INSERT OR IGNORE INTO Receipts (receipt_id, user_id, date, date_scanned, total_spent, rewardsReceiptStatus)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (row['_id'], row['userId'], row['createDate'], row['dateScanned'], row['totalSpent'], row['rewardsReceiptStatus']))

    # Insert receipt items
        
        for item in row['rewardsReceiptItemList']:
            barcode = item['barcode'] if 'barcode' in item and pd.notnull(item['barcode']) else item.get('userFlaggedBarcode', '4011')
            item_price = item['itemPrice'] if 'itemPrice' in item and pd.notnull(item['itemPrice']) else item.get(0)
            quantity = item['quantityPurchased'] if 'quantityPurchased' in item and pd.notnull(item['quantityPurchased']) else item.get(1)

            cursor.execute('''
            INSERT OR IGNORE INTO Receipt_Items (receipt_id, barcode, item_price, quantity, rewardsReceiptStatus)
            VALUES (?, ?, ?, ?, ?)
            ''', (row['_id'], barcode, item_price,quantity, row['rewardsReceiptStatus']))
    except Exception as e:
        print(f"Error inserting row {index} into receipt table: {e}, Data: {row}")

conn.commit()

# Verify data in the tables
def verify_table(table_name):
    cursor.execute(f'SELECT * FROM {table_name} LIMIT 5')
    print(f'\nData from {table_name}:')
    for row in cursor.fetchall():
        print(row)
        
        
print("\n[2]Write queries that directly answer predetermined questions from a business stakeholder:")
print("\n\n1) What are the top 5 brands by receipts scanned for most recent month?")       
top_5_brands_recent_month = cursor.execute('''
SELECT b.name, COUNT(r.receipt_id) as receipt_count
FROM Receipts r
JOIN Receipt_Items ri ON r.receipt_id = ri.receipt_id
JOIN Brands b ON ri.barcode = b.barcode
WHERE strftime('%Y-%m', r.date_scanned) = strftime('%Y-%m', 'now')
GROUP BY b.name
ORDER BY receipt_count DESC
LIMIT 5;
''').fetchall()
print("Top 5 brands by receipts scanned for the most recent month:", top_5_brands_recent_month)
print("Here we cannot find the recent month's data because we are unable to fetch the data for the most recent month as data is outdated")


print("\n2) How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?")

top_5_brands_previous_month = cursor.execute('''
SELECT b.name, COUNT(r.receipt_id) as receipt_count, 'Previous Month' as month
FROM Receipts r
JOIN Receipt_Items ri ON r.receipt_id = ri.receipt_id
JOIN Brands b ON ri.barcode = b.barcode
WHERE strftime('%Y-%m', r.date_scanned) = strftime('%Y-%m', date('now', '-1 month'))
GROUP BY b.name
ORDER BY receipt_count DESC
LIMIT 5;
''').fetchall()
print("Top 5 brands by receipts scanned for the previous month:", top_5_brands_previous_month)
print("Here we cannot find the previous month's data because we are unable to fetch the data for the most recent month as data is outdated")

print("\n3) When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?")
avg_spend_accepted_rejected = cursor.execute('''
SELECT rewardsReceiptStatus, AVG(total_spent) as avg_spent
FROM Receipts
WHERE rewardsReceiptStatus IN ('FINISHED', 'REJECTED')
GROUP BY rewardsReceiptStatus;
''').fetchall()
print("Average spend from receipts with 'Accepted' or 'Rejected' status:", avg_spend_accepted_rejected)

print("\n4) When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?")
total_items_accepted_rejected = cursor.execute('''
SELECT r.rewardsReceiptStatus, SUM(ri.quantity) as total_items
FROM Receipts r
JOIN Receipt_Items ri ON r.receipt_id = ri.receipt_id
WHERE r.rewardsReceiptStatus IN ('FINISHED', 'REJECTED')
GROUP BY r.rewardsReceiptStatus;
''').fetchall()
print("Total number of items purchased from receipts with 'Accepted' or 'Rejected' status:", total_items_accepted_rejected)


print("\n5) Which brand has the most spend among users who were created within the past 6 months?")
most_spent_brand_last_6_months = cursor.execute('''
SELECT b.name, SUM(r.total_spent) as total_spent
FROM Receipts r
JOIN Receipt_Items ri ON r.receipt_id = ri.receipt_id
JOIN Brands b ON ri.barcode = b.barcode
JOIN Users u ON r.user_id = u.user_id
WHERE u.created_date >= date('now', '-6 months')
GROUP BY b.name
ORDER BY total_spent DESC
LIMIT 1;
''').fetchall()
print("\nBrand with the most spend among users created within the past 6 months:", most_spent_brand_last_6_months)
print("We cannot find the brand with the most spend among users created within the past 6 months because the most recent data is dated: 2021-02-12")

print("\n6) Which brand has the most transactions among users who were created within the past 6 months?")
most_transactions_brand_last_6_months = cursor.execute('''
SELECT b.name, COUNT(r.receipt_id) as transaction_count
FROM Receipts r
JOIN Receipt_Items ri ON r.receipt_id = ri.receipt_id
JOIN Brands b ON ri.barcode = b.barcode
JOIN Users u ON r.user_id = u.user_id
WHERE u.created_date >= date('now', '-6 months')
GROUP BY b.name
ORDER BY transaction_count DESC
LIMIT 1;
''').fetchall()
print("\nBrand with the most transactions among users created within the past 6 months:", most_transactions_brand_last_6_months)
print("We cannot find the brand with the most transactions among users created within the past 6 months because the most recent data is dated: 2021-02-12")

print("\n[3]Evaluate Data Quality Issues in the Data Provided:")

# Check for missing values
print("\nMissing values for Users", users_df.isnull().sum())
print("\nMissing values for Receipts", receipts_df.isnull().sum())
print("\nMissing values for Brands", brands_df.isnull().sum())

# Check for inconsistent data types or invalid data
print("\nInconsistent values for Users", users_df.dtypes)
print("\nInconsistent values for Receipts", receipts_df.dtypes)
print("\nInconsistent values for Brands", brands_df.dtypes)


conn.close()  # Close the connection
