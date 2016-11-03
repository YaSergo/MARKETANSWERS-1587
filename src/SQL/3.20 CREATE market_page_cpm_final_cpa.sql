DROP TABLE IF EXISTS medintsev.market_page_cpm_final_cpa;
CREATE TABLE medintsev.market_page_cpm_final_cpa
AS
SELECT -- запрос рассчитывает среднее CPM каждой страницы (page_groupid_1 + page_groupid_2),
	   -- считает сколько чисел использовалось для расчёта среднего значения
	   -- рассчитывает среднеквадратичное отклонение для чисел используемых для расчёта среднего
	page_groupid_1, page_groupid_2, device, geo_id,
	AVG(CPM_visit) as CPM_avg,
	COUNT(*) as n,
	sum(if(CPM_visit > 0, 1, 0)) as n_gtz,	-- количество визитов у которых CPM было больше ноля
	stddev_samp(CPM_visit) as sd
FROM
	(
	SELECT -- запрос рассчитывает CPM для каждого визита
		visit_id, page_groupid_1, page_groupid_2, device, geo_id,
		AVG(pre_CPM) as CPM_visit
	FROM
		(
		SELECT -- запрос считает кумулятивную сумму
			*,
			SUM(price) OVER (PARTITION BY visit_id ORDER BY event_time DESC) as pre_CPM
		FROM market_page_cpm_daily_table20_cpa
		) t
	WHERE event = 'access'
	GROUP BY
		visit_id, page_groupid_1, page_groupid_2, device, geo_id
	) t
GROUP BY
	page_groupid_1, page_groupid_2, device, geo_id;