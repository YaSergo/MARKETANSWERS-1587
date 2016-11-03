DROP TABLE IF EXISTS medintsev.pre_II_final;
CREATE TABLE medintsev.pre_II_final
AS
SELECT -- запрос рассчитывает CPM для каждого визита
	visit_id, page_groupid_1, page_groupid_2, device, geo_id,
	AVG(pre_CPM) as CPM_visit
FROM
	(
	SELECT -- запрос считает кумулятивную сумму
		*,
		SUM(price) OVER (PARTITION BY visit_id ORDER BY event_time DESC) as pre_CPM
	FROM market_page_cpm_daily_table20
	) t
WHERE event = 'access'
GROUP BY
	visit_id, page_groupid_1, page_groupid_2, device, geo_id