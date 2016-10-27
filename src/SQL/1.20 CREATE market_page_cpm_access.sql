DROP TABLE IF EXISTS medintsev.market_page_cpm_access20;
CREATE TABLE medintsev.market_page_cpm_access20
COMMENT 'access data for cpm calculation'
AS
SELECT *
FROM
(
	SELECT
		*,
		LAG(page_groupid_1) OVER (PARTITION BY yandexuid ORDER BY access_time) AS prev_page_groupid_1,
		LAG(page_groupid_2) OVER (PARTITION BY yandexuid ORDER BY access_time) AS prev_page_groupid_2
	FROM (
		SELECT
		unix_timestamp(eventtime) AS access_time,	-- преобразует время из московского пояса в пояс UTC в формате unix time
		request,				-- для отладки
		request_path,
		yandexuid,	-- as cookie - для совместимости со старыми запросами

		CASE
		-- У продукта может быть не стандартная страница:
		-- https://market.yandex.ru/product--htc-desire-601/10533917
		WHEN regexp_extract(request_path, '^/product.*/\\d+', 0) <> ''
		THEN 1
		WHEN regexp_extract(request_path, '^/catalog/\\d+/list', 0) <> ''
		THEN 2
		WHEN regexp_extract(request_path, '^/catalog/\\d+', 0) <> ''
		THEN 3
		-- условия ниже можно сделать более строгими указав тип ID
		WHEN regexp_extract(request_path, '^/offer/', 0) <> ''
		THEN 4
		WHEN regexp_extract(request_path, '^/collections/', 0) <> ''
		THEN 5
		WHEN regexp_extract(request_path, '^/brands/', 0) <> ''
		THEN 6
		WHEN regexp_extract(request_path, '^/articles/', 0) <> ''
		THEN 7
		ELSE 0
		END as page_groupid_1,

		-- Вводим второй id для группировки при расчёте CPM
		-- Можно было бы просто использовать request_path, но я не уверен,
		-- что сейчас нет и в дальнейшем не будет подобных ситуаций:
		-- https://market.yandex.ru/product--htc-desire-601/10533917
		-- https://market.yandex.ru/product/10533917
		-- другая причина: есть request'ы типы /product/10437313/reviews
		-- которые должны относиться к продукту с id 10437313
		-- поэтому считаю целесообразно извлечь из request id

		-- medintsev 20160929: закомментировал предыдущий вариант
		-- CASE
		-- WHEN regexp_extract(request_path, '^/product.*/\\d+', 0) <> ''
		-- THEN concat('product_', regexp_extract(request_path, '^/product.*/(\\d+)', 1))
		-- WHEN regexp_extract(request_path, '^/catalog/\\d+/list', 0) <> ''
		-- THEN concat('cataloglist_', regexp_extract(request_path, '^/catalog/(\\d+)/list', 1))
		-- WHEN regexp_extract(request_path, '^/catalog/\\d+', 0) <> ''
		-- THEN concat('catalog_', regexp_extract(request_path, '^/catalog/(\\d+)', 1))
		-- -- условия ниже можно сделать более строгими указав тип ID
		-- WHEN regexp_extract(request_path, '^/offer/', 0) <> ''
		-- THEN concat('offer_', regexp_extract(request_path, '^/offer/(.*)', 1))
		-- WHEN regexp_extract(request_path, '^/collections/', 0) <> ''
		-- THEN concat('collections_', regexp_extract(request_path, '^/collections/(.*)', 1))
		-- WHEN regexp_extract(request_path, '^/brands/', 0) <> ''
		-- THEN concat('brands_', regexp_extract(request_path, '^/brands/(.*)', 1))
		-- WHEN regexp_extract(request_path, '^/articles/', 0) <> ''
		-- THEN concat('articles_', regexp_extract(request_path, '^/articles/(.*)', 1))
		-- ELSE NULL
		-- END as page_groupid_2,

		CASE
			WHEN regexp_extract(request_path, '^/product.*/\\d+', 0) <> ''
			THEN regexp_extract(request_path, '^/product.*/(\\d+)', 1)
			WHEN regexp_extract(request_path, '^/catalog/\\d+/list', 0) <> ''
			THEN regexp_extract(request_path, '^/catalog/(\\d+)/list', 1)
			WHEN regexp_extract(request_path, '^/catalog/\\d+', 0) <> ''
			THEN regexp_extract(request_path, '^/catalog/(\\d+)', 1)
			-- условия ниже можно сделать более строгими указав тип ID
			WHEN regexp_extract(request_path, '^/offer/', 0) <> ''
			THEN regexp_extract(request_path, '^/offer/(.*)', 1)
			WHEN regexp_extract(request_path, '^/collections/', 0) <> ''
			THEN regexp_extract(request_path, '^/collections/(.*)', 1)
			WHEN regexp_extract(request_path, '^/brands/', 0) <> ''
			THEN regexp_extract(request_path, '^/brands/(.*)', 1)
			WHEN regexp_extract(request_path, '^/articles/', 0) <> ''
			THEN regexp_extract(request_path, '^/articles/(.*)', 1)
			ELSE 0
		END as page_groupid_2,

		-- Группировка, которая использовалась Денисом:
		CASE
		WHEN request like '/catalog/%/list?%' AND (locate('?hid=', request) > 1 OR locate('&hid=', request) > 1)
		THEN 1     -- нижний уровень не в гуру категорий
		WHEN request like '/catalog/%'
		THEN 2     -- каталог
		WHEN request like '/catalogmodels.xml?%' AND (locate('?hid=', request) > 1 OR locate('&hid=', request) > 1)
		THEN 3     -- нижний уровень в иерархии гуру
		WHEN request like '/search%'
		AND (locate('?hid=', request) > 1 OR locate('&hid=', request) > 1)
		AND (locate('?nid=', request) > 1 OR locate('&nid=', request) > 1)
		THEN 4     -- результаты поиска
		WHEN request like '/product/%'
		THEN 5     -- карточка модели или кластера
		ELSE 0
		END as page_type

		FROM
		(
			SELECT *, parse_url(concat('https://market.yandex.ru', request), 'PATH') as request_path
			FROM robot_market_logs.front_access
			WHERE
				-- 21 день, в визитах будет 20
				day >= '2016-10-01'
				AND day <= '2016-10-21'
				AND yandexuid IS NOT NULL
		) t
	) t
) t
WHERE
	-- оставляем только данные о посещении классифицированных страниц
	page_groupid_1 > 0
	-- если не выполнится следующее условие, то считаем действие рефрешем и исключаем из выгрузки
	AND (page_groupid_1 <> nvl(prev_page_groupid_1,-1) OR page_groupid_2 <> nvl(prev_page_groupid_2,-1))