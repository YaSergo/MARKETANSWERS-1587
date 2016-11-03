DROP TABLE IF EXISTS medintsev.market_page_cpm_visits20;
CREATE TABLE medintsev.market_page_cpm_visits20
      COMMENT 'visits for cpm calculation'
AS
SELECT
	visit_id as visit_id,
	utc_start_time as visit_start_time,
	duration as visit_duration,
	user_id as yandexuid,
	region_id as geo_id,
	CASE
		WHEN NOT is_mobile AND NOT is_tablet AND NOT is_tv THEN 'desktop'
		WHEN is_tv THEN 'tv'
		WHEN is_tablet THEN 'tablet'
		WHEN is_mobile THEN 'mobile'
		ELSE 'other'
	END as device,
	mobile_phone as vendor,
	user_agent as ua
FROM
	robot_market_logs.visits
WHERE
	day >= '2016-10-01'
	AND day <= '2016-10-20'
	-- зачем нам в таблицу добавлять данные, которые не подвязываются...
	AND user_id IS NOT NULL