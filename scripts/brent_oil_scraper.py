"""
- Updates local Brent Oil historical data CSV from investing.com
- Scrapes the latest rows (all columns)
- Appends only missing rows by comparing dates
- Sorts the final CSV with the newest date on top
"""

import os
import shutil
import time
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

# --- CONFIG ---
URL = "https://www.investing.com/commodities/brent-oil-historical-data"
CSV_PATH = r"C:\\Users\\gpaghava\\Desktop\\Gulf fuel prices analysis\\Brent Oil Futures Historical Data.csv"
BACKUP_PATH = CSV_PATH + ".bak"
COLUMNS = ["Date", "Price", "Open", "High", "Low", "Vol.", "Change %"]

# --- Scraping via requests ---
def get_table_via_requests():
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(URL, headers=headers, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    table = soup.find("table")
    if not table:
        raise RuntimeError("No table found in HTML via requests.")
    rows = table.find_all("tr")

    data = []
    for row in rows[1:]:
        cols = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cols) == len(COLUMNS):
            data.append(cols)
    if not data:
        raise RuntimeError("No rows extracted via requests.")
    return pd.DataFrame(data, columns=COLUMNS)

# --- Selenium fallback ---
def get_table_via_selenium(headless=True):
    options = Options()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.get(URL)
    time.sleep(5)

    rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
    data = []
    for row in rows:
        tds = [td.text.strip() for td in row.find_elements(By.TAG_NAME, "td")]
        if len(tds) == len(COLUMNS):
            data.append(tds)
    driver.quit()

    if not data:
        raise RuntimeError("No rows extracted via Selenium.")
    return pd.DataFrame(data, columns=COLUMNS)

# --- Normalize data types ---
def normalize_dataframe(df):
    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date

    def clean_numeric_col(s):
        return (
            s.astype(str)
            .str.replace(",", "", regex=False)
            .str.replace("%", "", regex=False)
            .str.strip()
            .replace({"": None, "nan": None, "None": None})
        )

    for col in ["Price", "Open", "High", "Low"]:
        cleaned = clean_numeric_col(df[col])
        df[col] = pd.to_numeric(cleaned, errors="coerce")

    df["Vol."] = df["Vol."].astype(str).str.strip()
    df["Change %"] = (
        df["Change %"].astype(str).str.replace("%", "", regex=False).str.strip()
    )
    df["Change %"] = pd.to_numeric(df["Change %"], errors="coerce")

    return df

# --- Main ---
def main():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV not found at {CSV_PATH}")

    old_df = pd.read_csv(CSV_PATH)
    old_df = normalize_dataframe(old_df)

    # Scrape fresh data
    try:
        new_df = get_table_via_requests()
    except Exception as e:
        print("Requests scraping failed, trying Selenium fallback:", e)
        new_df = get_table_via_selenium()
    new_df = normalize_dataframe(new_df)

    # Filter new rows
    existing_dates = set(old_df["Date"].dropna())
    new_df = new_df[new_df["Date"].notnull()]
    rows_to_append = new_df[~new_df["Date"].isin(existing_dates)]

    if rows_to_append.empty:
        print("✅ No new rows to append — CSV is already up to date.")
        return

    # Append and sort by Date DESC (newest first)
    print(f"📅 Appending {len(rows_to_append)} new rows...")
    updated_df = pd.concat([old_df, rows_to_append], ignore_index=True)
    updated_df = updated_df.sort_values("Date", ascending=False).reset_index(drop=True)

    # Backup and save
    shutil.copy2(CSV_PATH, BACKUP_PATH)
    to_save = updated_df.copy()
    to_save["Date"] = pd.to_datetime(to_save["Date"]).dt.strftime("%Y-%m-%d")
    to_save.to_csv(CSV_PATH, index=False)
    print(f"✅ CSV updated and saved to {CSV_PATH}")
    print(f"🗂 Backup saved to {BACKUP_PATH}")
    print("⬇️ Newest data is now at the top of your CSV.")

if __name__ == "__main__":
    main()

