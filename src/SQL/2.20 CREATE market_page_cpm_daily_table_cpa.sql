DROP TABLE IF EXISTS medintsev.market_page_cpm_daily_table20_cpa;
CREATE TABLE medintsev.market_page_cpm_daily_table20_cpa
COMMENT 'daily data for cpm calculations'
-- в данной табличке удобно смотреть что происходило в течении визита
AS
SELECT
	visits.*,
	'click' as event,
	clicks.click_time as event_time,
    clicks.price,
    clicks.pp,
    clicks.hyper_id,
	NULL as request,
	NULL as request_path,
	NULL as page_groupid_1,
	NULL as page_groupid_2,
	NULL as page_type
FROM
	medintsev.market_page_cpm_visits20 visits LEFT JOIN medintsev.market_page_cpm_clicks20_cpa clicks
	ON visits.yandexuid = clicks.yandexuid
WHERE
	clicks.click_time >= visits.visit_start_time
	AND clicks.click_time <  visits.visit_start_time + visits.visit_duration

UNION ALL

SELECT
	visits.*,
	'access' as event,
	access_time as event_time,
    0 as price,
    NULL as pp,
    IF(page_groupid_1 = 1, page_groupid_2, NULL) as hyper_id, -- ещё для каких-либо типов страниц это условие выполняется?
	request,
	request_path,
	page_groupid_1,
	page_groupid_2,
	page_type
FROM
	medintsev.market_page_cpm_visits20 visits LEFT JOIN medintsev.market_page_cpm_access20 access
	ON visits.yandexuid = access.yandexuid
WHERE
	-- -60 - попытка исправить лаг при записи в access
	-- в таблице front_access встречаются записи, которые на несколько секунд раньше начала визита
  -- подробности в презентации https://st.yandex-team.ru/MARKETANSWERS-1587#1475762493000
	access.access_time >= (visits.visit_start_time - 60)
    AND access.access_time <  visits.visit_start_time + visits.visit_duration