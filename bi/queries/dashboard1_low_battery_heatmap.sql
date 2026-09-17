-- Dashboard 1: Bản đồ nhiệt xe cạn pin (min_soc <= 20%) theo khu vực/khung giờ.
-- Input Gold: fact_vehicle_daily_state + fact_vehicle_telemetry (Silver).
-- Cell lưới ~0.05 độ (~5km) để heatmap.
SELECT CAST(lat*20 AS INT)/20.0  AS lat_cell,
       CAST(lon*20 AS INT)/20.0  AS lon_cell,
       hour                     AS hour_vn,
       COUNT(*)                 AS low_battery_pings,
       COUNT(DISTINCT device_id) AS vehicles_low_batt
FROM fact_vehicle_telemetry
WHERE business_date = :business_date AND is_low_battery
GROUP BY 1,2,3 ORDER BY low_battery_pings DESC;
