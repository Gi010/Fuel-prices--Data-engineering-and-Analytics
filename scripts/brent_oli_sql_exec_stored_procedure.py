import pyodbc

# Connection settings
server = r"GPAGHAVA\GPAGAVA"
database = "OilDataWarehouse"

# Create connection using Windows Authentication
conn = pyodbc.connect(
    rf"DRIVER={{ODBC Driver 17 for SQL Server}};"
    rf"SERVER={server};"
    rf"DATABASE={database};"
    rf"Trusted_Connection=yes;"
)

cursor = conn.cursor()

try:
    # Execute the stored procedure
    cursor.execute("EXEC bronze.load_brent_oil")

    # Commit if stored procedure does INSERT/UPDATE/DELETE
    conn.commit()

    print("Stored procedure executed successfully.")

except Exception as e:
    print("Error executing stored procedure:", e)

finally:
    cursor.close()
    conn.close()

