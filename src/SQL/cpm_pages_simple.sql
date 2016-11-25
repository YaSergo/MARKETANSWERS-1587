set start_date=      '2016-11-20';
set end_date=        '2016-11-20';
set end_date_visits= '2016-11-20';

WITH access_orig AS (
  SELECT -- выбираем из access данные за требуемое время
    eventtime,
    request,
    yandexuid,
    parse_url(concat('https://market.yandex.ru', request), 'PATH') AS request_path
  FROM robot_market_logs.front_access
  WHERE
    day BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date} AND
    -- hour = 4 AND -- ДЛЯ ОТЛАДКИ!
    nvl(yandexuid, '') <> '' AND
    -- страница загружена без ошибок
    -- подробности: https://st.yandex-team.ru/MARKETANSWERS-1587#1478519919000
    status = '200'

), access_with_page_groupids AS (
  SELECT -- добавляем группы страниц (page_groupid_1 и page_groupid_2)
    access_time,
    request,
    yandexuid,
    page_groupid_1,
    page_groupid_2
  FROM
  (
    SELECT
      unix_timestamp(eventtime) AS access_time, -- преобразует время из московского пояса в пояс UTC в формате unix time
      request,
      yandexuid,

      CASE
        -- подробнее про группы страниц: https://nda.ya.ru/3SEEWx
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
      END as page_groupid_2

    FROM access_orig
  ) t
  WHERE page_groupid_1 > 0
  AND page_groupid_2 = 13584123 -- ДЛЯ ОТЛАДКИ!!!

), access AS (
  SELECT -- удаляем перезагрузку страниц
    access_time,
    request,
    yandexuid,
    page_groupid_1,
    page_groupid_2
  FROM
  (
    SELECT
      access_time,
      request,
      yandexuid,
      page_groupid_1,
      page_groupid_2,
      LAG(page_groupid_1) OVER (PARTITION BY yandexuid ORDER BY access_time) AS prev_page_groupid_1,
      LAG(page_groupid_2) OVER (PARTITION BY yandexuid ORDER BY access_time) AS prev_page_groupid_2
    FROM access_with_page_groupids
  ) t
  WHERE
    -- убираем перезагрузку страниц
    page_groupid_1 <> nvl(prev_page_groupid_1,-1) OR page_groupid_2 <> nvl(prev_page_groupid_2,-1)

), clicks_cpa AS (
  SELECT
    unix_timestamp(eventtime) as click_time, -- время клика
    cookie as yandexuid, -- id пользователя
    offer_price*fee*0.05 AS price  -- Цена клика в рублях с учетом CPA коэффициента
    -- 0.05 оценочное значение, после запуска https://paste.yandex-team.ru/170192
    -- https://st.yandex-team.ru/MARKETANSWERS-1587#1477662737000
  FROM robot_market_logs.cpa_clicks
  WHERE
    day BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date} AND
    nvl(filter, 0) = 0 AND  -- убираю накрутку
    state = 1 AND -- убираю действия сотрудников яндекса
    nvl(type_id, 0) = 0 AND -- только нажатия на кнопку в корзину
    nvl(cookie, '') <> '' -- указан yandexuid

), clicks_cpc AS (
  SELECT
    unix_timestamp(eventtime) AS click_time,
    cookie AS yandexuid,
    price*30/100 AS price  -- Цена клика в рублях
  FROM robot_market_logs.clicks
  WHERE
    day BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date} AND
    nvl(filter, 0) = 0 AND  -- убираю накрутку
    state = 1 AND -- убираю действия сотрудников яндекса
    nvl(cookie, '') <> '' -- указан yandexuid

), visits AS (
  SELECT
    visit_id as visit_id,
    utc_start_time as visit_start_time,
    duration as visit_duration,
    user_id as yandexuid
  FROM
    robot_market_logs.visits
  WHERE
    day BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date_visits} AND
    nvl(user_id, '') <> '' -- указан yandexuid

), visits_details AS (
  SELECT -- видно что происходило внутри визита (загрузка страниц + клики)
    visits.visit_id,
    visits.visit_start_time,
    visits.visit_duration,
    visits.yandexuid,

    'cpa click' as event,
    clicks_cpa.click_time as event_time,
    clicks_cpa.price,
    NULL as request,
    NULL as page_groupid_1,
    NULL as page_groupid_2
  FROM
    visits LEFT JOIN clicks_cpa
      ON visits.yandexuid = clicks_cpa.yandexuid
  WHERE
    clicks_cpa.click_time >= visits.visit_start_time AND
    clicks_cpa.click_time <  visits.visit_start_time + visits.visit_duration

  UNION ALL

  SELECT
    visits.visit_id,
    visits.visit_start_time,
    visits.visit_duration,
    visits.yandexuid,

    'cpc click' as event,
    clicks_cpc.click_time as event_time,
    clicks_cpc.price,
    NULL as request,
    NULL as page_groupid_1,
    NULL as page_groupid_2
  FROM
    visits LEFT JOIN clicks_cpc
      ON visits.yandexuid = clicks_cpc.yandexuid
  WHERE
    clicks_cpc.click_time >= visits.visit_start_time AND
    clicks_cpc.click_time <  visits.visit_start_time + visits.visit_duration

  UNION ALL

  SELECT
    visits.visit_id,
    visits.visit_start_time,
    visits.visit_duration,
    visits.yandexuid,

    'access' as event,
    access_time as event_time,
    0 as price,
    request,
    page_groupid_1,
    page_groupid_2
  FROM
    visits LEFT JOIN access
    ON visits.yandexuid = access.yandexuid
  WHERE
    -- -60 - попытка исправить лаг при записи в access
    -- в таблице front_access встречаются записи, которые на несколько секунд раньше начала визита
    -- подробности в презентации https://st.yandex-team.ru/MARKETANSWERS-1587#1475762493000
    access.access_time >= (visits.visit_start_time - 60)
      AND access.access_time <  visits.visit_start_time + visits.visit_duration

), cpm_per_visits AS (
  SELECT -- запрос рассчитывает CPM для каждого визита
    visit_id,
    page_groupid_1,
    page_groupid_2,
    AVG(pre_CPM) as cpm_visit
  FROM
    (
      SELECT -- запрос считает кумулятивную сумму
        visit_id,
        page_groupid_1,
        page_groupid_2,
        event,
        SUM(price) OVER (PARTITION BY visit_id ORDER BY event_time DESC) as pre_CPM
      FROM visits_details
    ) t
  WHERE event = 'access'
  GROUP BY
    visit_id,
    page_groupid_1,
    page_groupid_2

), cpm_per_page_groupids AS (
  SELECT
    page_groupid_1,
    page_groupid_2,
    AVG(CPM_visit) as CPM_avg,
    collect_list(CPM_visit) as cpms, -- для ОТЛАДКИ
    COUNT(*) as n,
    sum(if(CPM_visit > 0, 1, 0)) as n_gtz,  -- количество визитов у которых CPM было больше ноля
    stddev_samp(CPM_visit) as sd
  FROM cpm_per_visits
  GROUP BY
    page_groupid_1,
    page_groupid_2
)

SELECT
  page_groupid_1,
  page_groupid_2,
  CPM_avg,
  n,
  n_gtz,  -- количество визитов у которых CPM было больше ноля
  sd
FROM
  cpm_per_page_groupids