SELECT agg.name name,agg.high high,date_add('hour',-4,CAST(ts AS timestamp)) datetime,agg.hour hour
FROM stream_stock_prices t
JOIN
(
SELECT name,MAX(high) high,hour(CAST(ts AS timestamp))-4 hour
FROM stream_stock_prices
GROUP BY name,hour(cast(ts as timestamp))
) agg
ON t.name=agg.name AND t.high=agg.high AND hour(CAST(ts AS timestamp))-4=hour
ORDER BY name,hour,datetime;
