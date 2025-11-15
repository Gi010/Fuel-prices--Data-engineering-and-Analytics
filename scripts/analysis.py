# Loading the data

import pandas as pd
import pyodbc

conn = pyodbc.connect(
    'Driver={SQL Server};'
    'Server=GPAGHAVA\\GPAGAVA;'
    'Database=OilDataWarehouse;'
    'Trusted_Connection=yes;'
)

query = """
SELECT
    g.sales_date AS [Date],
    g.premium AS Fuel_price,
    bo.price AS Oil_price,
    cr.rate AS Currency_rate
FROM bronze.gulf g
LEFT JOIN bronze.brent_oil bo
    ON bo.trade_date = g.sales_date
LEFT JOIN bronze.currency_rates cr
    ON cr.trade_date = g.sales_date
WHERE bo.price IS NOT NULL
"""

df = pd.read_sql(query, conn)

df.head()


# Basic data quality checks

print(df.describe())
print(df.isna().sum())

# Correlation check
"""
Correlation 0.7–1.0 → strong relationship

Correlation 0.3–0.7 → medium

Correlation <0.3 → weak
"""
df.drop('Date', axis = 1).corr()

# Regression model

import statsmodels.api as sm

X = df[['Oil_price', 'Currency_rate']]
X = sm.add_constant(X)
y = df['Fuel_price']

model = sm.OLS(y, X).fit()
print(model.summary())

# Evaluate model with RMSE

from sklearn.metrics import mean_squared_error
import numpy as np

pred = model.predict(X)
rmse = np.sqrt(mean_squared_error(y, pred))
rmse

# Predict future fuel prices
"""
Case scenario: Brent oil price increases to 90 and GEL to USD currency rate increases to 2.75
"""
model.params.index
new_data = pd.DataFrame({
    'const': [1],
    'Oil_price': [90],
    'Currency_rate': [2.75]
})
new_data = sm.add_constant(new_data)

predicted_price = model.predict(new_data)
predicted_price

# Visualizations

import matplotlib.pyplot as plt

# Fuel vs Oil

plt.scatter(df['Oil_price'], df['Fuel_price'])
plt.plot(df['Oil_price'], pred, linewidth=2)
plt.xlabel('Oil Price')
plt.ylabel('Fuel Price')
plt.show()

# Fuel vs Currency rates

plt.scatter(df['Currency_rate'], df['Fuel_price'])
plt.plot(df['Currency_rate'], pred, linewidth=2)
plt.xlabel('Currency Rate')
plt.ylabel('Fuel Price')
plt.show()
