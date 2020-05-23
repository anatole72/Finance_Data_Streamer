SELECT name,MAX(high) high,date(CAST(ts AS timestamp)) date,hour(CAST(ts AS timestamp))-4 hour
FROM stream_stock_prices
GROUP BY name,date(CAST(ts AS timestamp)),hour(cast(ts as timestamp))
ORDER BY name,hour;
