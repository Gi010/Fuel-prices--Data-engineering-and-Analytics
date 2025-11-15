import requests
import pandas as pd
from datetime import datetime
import pyodbc

EXCEL_PATH = r"C:\Users\gpaghava\Desktop\Gulf fuel prices analysis\gel_to_usd_rates_2021_present.xlsx"
NBG_URL = "https://nbg.gov.ge/gw/api/ct/monetarypolicy/currencies/en/json"

def parse_iso_date(s):
    """Parse various ISO-like date strings to a datetime object (naive)."""
    if s is None:
        return None
    s = s.strip()
    # remove trailing Z if present
    if s.endswith("Z"):
        s = s[:-1]
    # some values might be like "2025-11-13T17:01:11.812" or "2025-11-14 00:00:00.000"
    try:
        return datetime.fromisoformat(s)
    except Exception:
        # fallback: try common patterns
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt)
            except Exception:
                continue
    return None

def get_usd_candidate_list(json_data):
    """
    Walk the JSON list and return candidate tuples (date_dt, rate)
    where date_dt is chosen from validFromDate (prefer) or date field.
    """
    candidates = []

    if not isinstance(json_data, list):
        raise ValueError("Unexpected JSON structure: expected a list at top level.")

    for entry in json_data:
        # Case A: entry is a container with "currencies": [...]
        if isinstance(entry, dict) and "currencies" in entry and isinstance(entry["currencies"], list):
            # the entry may have its own 'date' or 'validFromDate' fields, but currencies may also have them
            container_date = None
            # try container-level validFromDate or date if exists
            if "validFromDate" in entry:
                container_date = parse_iso_date(entry.get("validFromDate"))
            elif "date" in entry:
                container_date = parse_iso_date(entry.get("date"))

            for cur in entry["currencies"]:
                try:
                    if cur.get("code") == "USD":
                        # prefer currency-level validFromDate if present
                        cand_date = None
                        if cur.get("validFromDate"):
                            cand_date = parse_iso_date(cur.get("validFromDate"))
                        elif cur.get("date"):
                            cand_date = parse_iso_date(cur.get("date"))
                        else:
                            cand_date = container_date
                        # rate may be numeric or a string — coerce to float
                        rate_val = cur.get("rate")
                        try:
                            rate_val = float(rate_val)
                        except Exception:
                            # maybe rate is nested in rateFormated
                            rf = cur.get("rateFormated")
                            rate_val = float(rf.replace(",", "")) if rf else None
                        candidates.append((cand_date, rate_val))
                except Exception:
                    continue

        # Case B: entry itself might be a currency object with "code"
        elif isinstance(entry, dict) and "code" in entry:
            try:
                if entry.get("code") == "USD":
                    # pick validFromDate or date on this object
                    cand_date = None
                    if entry.get("validFromDate"):
                        cand_date = parse_iso_date(entry.get("validFromDate"))
                    elif entry.get("date"):
                        cand_date = parse_iso_date(entry.get("date"))
                    rate_val = entry.get("rate")
                    try:
                        rate_val = float(rate_val)
                    except Exception:
                        rf = entry.get("rateFormated")
                        rate_val = float(rf.replace(",", "")) if rf else None
                    candidates.append((cand_date, rate_val))
            except Exception:
                continue
        else:
            # Unknown entry shape — ignore
            continue

    return candidates

def get_latest_usd_rate_and_date():
    """Main helper: return (date_str_mmddyyyy, rate_float) or raise if not found."""
    resp = requests.get(NBG_URL, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    candidates = get_usd_candidate_list(data)
    if not candidates:
        raise ValueError("No USD candidates found in JSON. Example JSON snippet needed.")

    # Filter out entries without dates or rates
    filtered = [(d, r) for (d, r) in candidates if r is not None and d is not None]
    if not filtered:
        # if no candidates have both date and rate, fall back to any with rate
        filtered = [(d, r) for (d, r) in candidates if r is not None]

    if not filtered:
        raise ValueError("USD entries found but none had usable date or rate.")

    # pick the candidate with the maximum date (most recent)
    # if date is None but rate not None, those will be after filtering; prefer those with date
    best = max(filtered, key=lambda x: x[0] if x[0] is not None else datetime.min)
    best_date_dt, best_rate = best

    if best_date_dt is None:
        # if still none, fall back to today
        best_date_dt = datetime.today()

    # convert to MM/DD/YYYY string
    excel_date_str = best_date_dt.strftime("%m/%d/%Y")
    return excel_date_str, float(best_rate)

def parse_mixed_dates(series):
    """Safely parse mixed-format date column to datetime (naive)."""
    return pd.to_datetime(series, errors="coerce", infer_datetime_format=True)

def append_rate_to_excel(excel_path, date_str_mmddyyyy, rate):
    """Append new row if date not present; write Excel back sorted newest first."""
    df = pd.read_excel(excel_path)

    # normalize existing dates to datetime
    df["Date_parsed"] = parse_mixed_dates(df["Date"])
    new_dt = datetime.strptime(date_str_mmddyyyy, "%m/%d/%Y")

    # check duplicates using parsed datetimes
    if new_dt in df["Date_parsed"].values:
        print("No new data — this date already exists in Excel.")
        return False

    # append new row (store Date as MM/DD/YYYY string to match your format)
    new_row = {"Date": date_str_mmddyyyy, "rate": rate}
    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

    # sort by parsed date descending
    df["Date_parsed"] = parse_mixed_dates(df["Date"])
    df = df.sort_values("Date_parsed", ascending=False).drop(columns=["Date_parsed"])

    # ensure Date column formatting MM/DD/YYYY
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%m/%d/%Y")

    # Save back to Excel
    df.to_excel(excel_path, index=False)
    print(f"Appended {date_str_mmddyyyy} -> {rate} to Excel.")
    return True

def load_excel_into_sql_server(excel_path):
    """Load the entire Excel file into SQL Server table bronze.currency_rates."""
    
    # 1. Read Excel
    df = pd.read_excel(excel_path)

    # Normalize date column
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
    df["rate"] = df["rate"].astype(float)

    # 2. Connect to SQL Server
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=GPAGHAVA\\GPAGAVA;"
        "DATABASE=OilDataWarehouse;"
        "Trusted_Connection=yes;"
    )

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # 3. Insert rows
    insert_sql = """
        INSERT INTO bronze.currency_rates (trade_date, rate)
        VALUES (?, ?)
    """

    for idx, row in df.iterrows():
        cursor.execute(insert_sql, row["Date"], row["rate"])

    conn.commit()
    cursor.close()
    conn.close()

    print("✔ All Excel rows loaded into SQL Server successfully.")


def main():
    try:
        date_str, rate = get_latest_usd_rate_and_date()
    except Exception as e:
        print("Failed to fetch USD rate from NBG JSON:", e)
        return

    try:
        success = append_rate_to_excel(EXCEL_PATH, date_str, rate)
        if success:
            print("Excel updated successfully.")
    except Exception as e:
        print("Failed to append to Excel:", e)
        return

    # NEW PART: load everything into SQL Server
    try:
        load_excel_into_sql_server(EXCEL_PATH)
    except Exception as e:
        print("Failed to load data into SQL Server:", e)


if __name__ == "__main__":
    main()
