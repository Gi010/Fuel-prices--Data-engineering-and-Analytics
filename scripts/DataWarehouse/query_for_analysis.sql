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