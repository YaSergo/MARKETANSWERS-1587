DROP TABLE IF EXISTS medintsev.pre_final;
CREATE TABLE medintsev.pre_final
AS
SELECT -- запрос считает кумулятивную сумму
	*,
	SUM(price) OVER (PARTITION BY visit_id ORDER BY event_time DESC) as pre_CPM
FROM market_page_cpm_daily_table20
-- ORDER BY
--   visit_start_time, visit_id, event_time,
--   event -- access перед click будет, если всё остальное совпадёт


DROP TABLE IF EXISTS medintsev.pre_final_cpa;
CREATE TABLE medintsev.pre_final_cpa
AS
SELECT -- запрос считает кумулятивную сумму
  *,
  SUM(price) OVER (PARTITION BY visit_id ORDER BY event_time DESC) as pre_CPM
FROM market_page_cpm_daily_table20_cpa
-- ORDER BY
--   visit_start_time, visit_id, event_time,
--   event -- access перед click будет, если всё остальное совпадёт