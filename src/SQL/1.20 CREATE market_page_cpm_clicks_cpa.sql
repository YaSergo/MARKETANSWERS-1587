DROP TABLE IF EXISTS medintsev.market_page_cpm_clicks20_cpa;
CREATE TABLE medintsev.market_page_cpm_clicks20_cpa
AS
SELECT
	unix_timestamp(eventtime) as click_time,
	cookie as yandexuid,              -- кука yandexuid или пустое значение
	pp,
	hyper_id,
	offer_price*fee AS original_price,			-- Исходная цена в рублях
	offer_price*fee*0.05/30*100 AS price  -- Цена клика (в фишка-центах) с учетом CPA коэффициента
  -- 0.05 оценочное значение, после запуска https://paste.yandex-team.ru/170192
  -- https://st.yandex-team.ru/MARKETANSWERS-1587#1477662737000
FROM robot_market_logs.cpa_clicks
WHERE
  day >= '2016-10-18' AND
  day <= '2016-11-16' AND
  nvl(filter, 0) = 0 AND
  state = 1 AND
  nvl(type_id, 0) = 0
  -- AND touch = 0 -- нас интересует только desktop клики
  -- зачем нам в таблицу добавлять данные, которые не подвязываются...
  AND nvl(cookie, '') <> ''