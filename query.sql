SELECT name,MAX(high) high,MAX(date_add('hour',-4,CAST(ts AS timestamp))) AS datetime,hour(CAST(ts AS timestamp))-4 hour
FROM stream_stock_prices
GROUP BY name,hour(cast(ts as timestamp))
ORDER BY name,hour;
