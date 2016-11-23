DROP TABLE IF EXISTS medintsev.market_page_cpm_clicks20;
CREATE TABLE medintsev.market_page_cpm_clicks20
	COMMENT 'clicks for cpm calculation'
AS
SELECT
	unix_timestamp(eventtime) as click_time,
	cookie as yandexuid,              -- кука yandexuid или пустое значение
	pp,
	hyper_id,
	price                -- Цена клика (в фишка-центах). Верхний предел определяется максимальной ценой клика, сейчас это 84 уе
FROM robot_market_logs.clicks
WHERE
	filter = 0     -- не накрутка
	AND state = 1        -- в документации ничего про это поле не сказано
	AND day >= '2016-10-18'
	AND day <= '2016-11-16'
	-- зачем нам в таблицу добавлять данные, которые не подвязываются...
	AND nvl(cookie, '') <> ''
