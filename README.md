# Fuel Price Analysis: Data Engineering & Analytics Project

This project combines data engineering and data analytics to explore how fuel prices in Georgia relate to international Brent oil prices and GEL/USD exchange rate fluctuations. Although the fuel price data is obtained from Gulf Georgia, one of the largest oil companies in the country, fuel prices across all major providers are nearly identical—making this dataset representative for the whole market.

---

## 📌 Project Objectives

Collect daily data on:
- Fuel prices (Gulf Georgia)
- Brent oil prices
- GEL/USD exchange rates

Build a Microsoft SQL Server data warehouse to store and process the data

Analyze the dependency of local fuel prices on:
- Global Brent oil trends
- Currency exchange rate movements

Develop predictive insights using regression analysis and other analytics techniques


## 📦 Data Engineering Workflow

**Data Warehouse (MS SQL Server)**

All data is stored in a structured MS SQL Server data warehouse, which supports:
- Historical storage
- Automated incremental updates
- Querying for analytics
- Integration with Python ETL workflows


## 🌐 Data Sources

1️⃣ **Fuel Prices — Gulf Georgia**

Source: https://www.gulf.ge

A Python script scrapes fuel prices starting from October 28, 2021 up to the present.
This script also automatically loads the data into MS SQL Server.

2️⃣ **Brent Oil Prices — Investing.com**

Source: https://www.investing.com

Process:
1. A Python scraper extracts daily Brent oil prices (LCOF6 futures)
2. The data is saved to a CSV file
3. Another Python script triggers a stored procedure in MS SQL Server
4. The stored procedure loads the latest oil price data into the data warehouse

Data range: From October 1, 2021 to present

3️⃣ **Exchange Rates — National Bank of Georgia**

Source: https://nbg.gov.ge

Using the official JSON API, a Python script:
- Retrieves the daily GEL/USD exchange rate
- Cleans and transforms the JSON payload
- Loads the data into the MS SQL warehouse
  
Data range: From October 1, 2021 to present


## 📊 Data Analytics Workflow

A dedicated Python analysis script:
- Joins fuel, oil, and exchange-rate data
- Performs exploratory data analysis (EDA)
- Studies correlations between variables
- Builds regression models to quantify relationships
- Visualizes trends and dependencies

Key questions analyzed:

- How closely do Georgian fuel prices follow Brent oil price movements?
- How much influence does the GEL/USD exchange rate have?
- Can we build a reliable predictive model based on historical behavior?


## 🛠️ Technologies Used

**Python**
- BeautifulSoup / Requests / Selenium (web scraping)
- Pandas / NumPy (data processing)
- Statsmodels / Scikit-learn (analytics)
- Matplotlib / Seaborn (visualization)

**MS SQL Server**
- Tables, stored procedures, and scheduled ETL loading

**CSV / JSON**
- Intermediate storage formats for scraped data


## 📈 Final Output

The analysis provides:
- Data trends
- Visuals
- Regression-based insights
- Predictive capability for future fuel prices based on oil and currency movements
