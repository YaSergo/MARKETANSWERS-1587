DROP TABLE IF EXISTS medintsev.market_page_cpm_clicks20_cpa;
CREATE TABLE medintsev.market_page_cpm_clicks20_cpa
AS
SELECT
	unix_timestamp(eventtime) as click_time,     -- в документации ничего про это поле не сказано
	cookie as yandexuid,              -- кука yandexuid или пустое значение
	pp,
	hyper_id,
	offer_price*fee AS original_price,			-- Исходная цена в рублях
	offer_price*fee*0.1392/30*100 AS price  -- Цена клика (в фишка-центах) с учетом CPA коэффициента
FROM robot_market_logs.cpa_clicks
WHERE
  day >= '2016-10-01' AND
  day <= '2016-10-21' AND
  nvl(filter, 0) = 0 AND
  state = 1 AND
  nvl(type_id, 0) = 0 AND
  touch = 0 -- нас интересует только desktop клики