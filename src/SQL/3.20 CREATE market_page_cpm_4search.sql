DROP TABLE IF EXISTS medintsev.market_page_cpm_4search20;
CREATE TABLE medintsev.market_page_cpm_4search20
	COMMENT 'значения CPM_S, которые планируется использовать для поиска'
AS
SELECT -- запрос рассчитывает среднее CPM_S каждой страницы (page_groupid_1 + page_groupid_2),
	   -- считает сколько чисел использовалось для расчёта среднего значения
	   -- рассчитывает среднеквадратичное отклонение для чисел используемых для расчёта среднего
	page_groupid_1,
	page_groupid_2,
	AVG(CPM_S_visit) as CPM_S_avg,
	COUNT(*) as n,
	sum(if(CPM_S_visit > 0, 1, 0)) as n_gtz,	-- количество визитов у которых CPM_S было больше ноля
	--STDDEV_POP(CPM_S_visit) as sd
	stddev_samp(CPM_S_visit) as sd
FROM
	(
	SELECT -- запрос рассчитывает CPM_S для каждого визита
		visit_id, hyper_id, page_groupid_1, page_groupid_2, AVG(pre_CPM_S) as CPM_S_visit
	FROM
		(
		SELECT -- запрос считает кумулятивную сумму
			*,
			SUM(price) OVER (PARTITION BY visit_id, hyper_id ORDER BY event_time DESC) as pre_CPM_S
		FROM market_page_cpm_daily_table20
		WHERE 
			-- или клики или page_groupid_1 == 1 (КМ)
			(event = 'click'
			OR (event = 'access'
				AND page_groupid_1 = 1))
		) t
	WHERE event = 'access'
	GROUP BY
		visit_id, hyper_id, page_groupid_1, page_groupid_2
	) t
GROUP BY
	page_groupid_1, page_groupid_2;