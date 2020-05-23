SELECT name,MAX(high) as high,substr(ts,1,11) as date,substr(ts,11,3) as hour
FROM stream_stock_prices
GROUP BY name,substr(ts,1,11),substr(ts,11,3)
ORDER BY name,Hour;
