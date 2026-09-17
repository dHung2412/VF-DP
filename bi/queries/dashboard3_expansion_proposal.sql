-- Dashboard 3: Tương quan xe mới bán vs công suất trạm -> đề xuất mở rộng.
-- Ưu tiên quận/huyện có nhiều xe bán mới nhưng utilization cao / ít cổng sạc.
WITH sales AS (
  SELECT dd.province, dd.city, SUM(f.vehicles_sold) AS sold
  FROM fact_distributor_monthly f JOIN dim_distributor dd USING (distributor_id)
  WHERE f.month BETWEEN :from_month AND :to_month GROUP BY 1,2
),
load AS (
  SELECT dd.city, AVG(fs.utilization_rate) AS avg_util,
         SUM(fs.total_charging_energy) AS kwh
  FROM fact_station_daily fs
  JOIN bridge_station_distributor b USING (station_id)
  JOIN dim_distributor dd USING (distributor_id)
  WHERE fs.business_date BETWEEN :from_date AND :to_date
  GROUP BY 1
)
SELECT s.province, s.city, s.sold,
       COALESCE(l.avg_util,0) AS avg_station_util,
       COALESCE(l.kwh,0)      AS total_kwh,
       CASE WHEN COALESCE(l.avg_util,0) >= 0.7 AND s.sold > 50 THEN 'P0_mo_tram_moi'
            WHEN COALESCE(l.avg_util,0) >= 0.5 AND s.sold > 20 THEN 'P1_bo_sung_tru'
            ELSE 'theo_doi' END AS proposal
FROM sales s LEFT JOIN load l USING (city)
ORDER BY s.sold DESC, avg_station_util DESC;
