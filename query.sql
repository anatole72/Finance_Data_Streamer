SELECT name,MAX(high) AS high,substr(ts,1,11) AS date,substr(ts,11,3) AS hour
FROM stream_stock_prices
GROUP BY name,substr(ts,1,11),substr(ts,11,3)
ORDER BY name,hour;
