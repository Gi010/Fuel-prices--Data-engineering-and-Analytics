import requests
from bs4 import BeautifulSoup
import pandas as pd
import pyodbc
from datetime import timedelta, datetime

# ---------- SQL CONFIG ----------
server = 'GPAGHAVA\GPAGAVA'   # e.g., 'DESKTOP-123ABC\\SQLEXPRESS'
database = 'OilDataWarehouse'  # e.g., 'fuel_data'
table = 'bronze.gulf'  # schema.table name

# ---------- SCRAPER ----------
base_url = 'https://gulf.ge/ge/fuel_prices?page='
table_attribs = ['Date', 'Super', 'Premium', 'G-Force Regular', 'Regular']

def extract_page(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            data_dict = {
                'Date': col[0].get_text(strip=True),
                'Super': col[1].get_text(strip=True),
                'Premium': col[2].get_text(strip=True),
                'G-Force Regular': col[3].get_text(strip=True),
                'Regular': col[4].get_text(strip=True)
            }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df


# --- SCRAPE PAGE 1 ---
result = extract_page(base_url, table_attribs)

# Convert 'Date' column to datetime
result['Date'] = pd.to_datetime(result['Date'], format='%Y-%m-%d')

# Sort by date ascending (oldest first)
result = result.sort_values('Date').reset_index(drop=True)

# Prepare list for expanded rows
expanded_rows = []

# Fill missing weekdays between dates (bottom-up)
for i in range(len(result) - 1, 0, -1):
    current_row = result.iloc[i]
    prev_row = result.iloc[i - 1]
    current_date = current_row['Date']
    prev_date = prev_row['Date']

    expanded_rows.append(current_row)

    # Fill weekdays between current_date and prev_date
    date_to_fill = current_date - timedelta(days=1)
    while date_to_fill > prev_date:
        if date_to_fill.weekday() < 5:  # Mon–Fri
            filled_row = current_row.copy()
            filled_row['Date'] = date_to_fill
            expanded_rows.append(filled_row)
        date_to_fill -= timedelta(days=1)

# Add the oldest record
expanded_rows.append(result.iloc[0])

# Combine and sort descending (latest first)
final_df = pd.DataFrame(expanded_rows)
final_df = final_df.sort_values('Date', ascending=False).reset_index(drop=True)

# --- EXTEND TO TODAY (SKIP WEEKENDS) ---
last_known_row = final_df.iloc[0].copy()
last_date = pd.to_datetime(final_df.iloc[0]['Date'], errors='coerce')
today = pd.Timestamp(datetime.now().date())

# Add missing weekdays until today
date_to_add = last_date + timedelta(days=1)
while date_to_add <= today:
    if date_to_add.weekday() < 5:  # Mon–Fri only
        new_row = last_known_row.copy()
        new_row['Date'] = date_to_add
        final_df = pd.concat([pd.DataFrame([new_row]), final_df], ignore_index=True)
    date_to_add += timedelta(days=1)

# ---------- CLEAN AND MAP TO SQL ----------
final_df.rename(columns={
    'Date': 'sales_date',
    'Super': 'super',
    'Premium': 'premium',
    'G-Force Regular': 'g_force_regular',
    'Regular': 'regular'
}, inplace=True)

# Ensure correct data types
final_df['sales_date'] = pd.to_datetime(final_df['sales_date'])
for col in ['super', 'premium', 'g_force_regular', 'regular']:
    final_df[col] = pd.to_numeric(final_df[col], errors='coerce')

# ---------- SQL CONNECTION ----------
conn_str = (
    'DRIVER={SQL Server};'
    f'SERVER={server};'
    f'DATABASE={database};'
    'Trusted_Connection=yes;'
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Optional: clear old data before inserting (comment out if you want to append instead)
cursor.execute("TRUNCATE TABLE bronze.gulf;")
conn.commit()

# ---------- INSERT DATA ----------
insert_query = """
INSERT INTO bronze.gulf (sales_date, super, premium, g_force_regular, regular)
VALUES (?, ?, ?, ?, ?)
"""

for _, row in final_df.iterrows():
    cursor.execute(insert_query, 
                   row['sales_date'], 
                   row['super'], 
                   row['premium'], 
                   row['g_force_regular'], 
                   row['regular'])
conn.commit()

cursor.close()
conn.close()

print("✅ Fuel prices expanded, extended to today, and saved to SQL Server successfully!")
