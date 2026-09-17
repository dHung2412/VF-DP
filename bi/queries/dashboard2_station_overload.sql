-- Dashboard 2: Hiệu suất & quá tải trạm sạc theo relationship_type.
SELECT s.station_id, b.relationship_type, d.business_date,
       d.total_charging, d.total_charging_time, d.total_charging_energy,
       d.utilization_rate,
       CASE WHEN d.utilization_rate >= 0.8 THEN 'overloaded'
            WHEN d.utilization_rate >= 0.5 THEN 'busy'
            ELSE 'normal' END AS load_status
FROM fact_station_daily d
JOIN dim_station s USING (station_id)
LEFT JOIN bridge_station_distributor b USING (station_id)
WHERE d.business_date BETWEEN :from_date AND :to_date
ORDER BY d.utilization_rate DESC;
