-- 12:24
-- === market_page_cpm_clicks20_cpa === --
SELECT
  to_date(from_unixtime(click_time)) as day, count(DISTINCT yandexuid) as num_yandexuid
FROM market_page_cpm_clicks20_cpa
GROUP BY
  to_date(from_unixtime(click_time))
-- данные представлены с 01 по 21 октября включительно
-- резких провалов и отсутсвия данных не обнаружил https://jing.yandex-team.ru/files/medintsev/Hue_-_Hive_Editor_-_Query_2016-10-27_12-50-14.png
-- по субботам и воскресеньям видно снижение количества уникальных yandexuid примерно на 1/3

-- === market_page_cpm_visits20 === --
SELECT
  to_date(from_unixtime(visit_start_time)) as day, count(DISTINCT yandexuid) as num_yandexuid
FROM market_page_cpm_visits20
GROUP BY
  to_date(from_unixtime(visit_start_time))
-- данные представлены с 01 по 20 октября включительно
-- наблюдаются резкие скачки данных: https://jing.yandex-team.ru/files/medintsev/Hue_-_Hive_Editor_-_Query_2016-10-27_12-48-13.png
-- что намекает на неполноту данных
-- <сслыка на описание проблемы в visits>

-- === market_page_cpm_access20 === --
SELECT
  to_date(from_unixtime(access_time)) as day, count(DISTINCT yandexuid) as num_yandexuid
FROM market_page_cpm_access20
GROUP BY
  to_date(from_unixtime(access_time))
-- данные представлены с 01 по 21 октября включительно
-- резких провалов и отсутсвия данных не обнаружил https://jing.yandex-team.ru/files/medintsev/Hue_-_Hive_Editor_-_Query_2016-10-27_12-53-45.png
-- по субботам и воскресеньям видно снижение количества уникальных yandexuid примерно на 1/5

-- === market_page_cpm_daily_table20_cpa === --

-- = 1 = --
-- оценка цены
SELECT -- тут должно получиться чуть больше, чем в запросе ниже (т.к. визит мог закончиться и 21 октября, а клики считаются только до 20 октября)
  sum(price) as price
FROM market_page_cpm_daily_table20_cpa
--  21'361'698
-- 20161117: 208'613'843

SELECT
  sum(offer_price*fee*0.05/30*100) AS price  -- Цена клика (в фишка-центах) с учетом CPA коэффициента
FROM robot_market_logs.cpa_clicks
WHERE
  day >= '2016-10-18' AND
  day <= '2016-11-15' AND
  nvl(filter, 0) = 0 AND
  state = 1 AND
  nvl(type_id, 0) = 0
  -- AND touch = 0 -- нас интересует только desktop клики
  -- зачем нам в таблицу добавлять данные, которые не подвязываются...
  AND nvl(cookie, '') <> ''
  --  75'410'084
  -- 20161117: 216'189'783 - отличие на 4% считаю допустимым, хотя тоже стоит уточнить...

  -- разница получилась значительной (нет 70% денег), вероятно из-за того, что не все визиты представлены в visits и многим кликам просто некуда подвязываться
-----------
SELECT
  good_groupid_2.page_groupid_2 as page_groupid_2,
  good_groupid_2.n_gtz as n_gtz_cpa,
  market_page_cpm_4search20_cpa.cpm_s_avg as CPM_CPA,
  market_page_cpm_4search20_cpa.n as n_CPA,
  market_page_cpm_4search20.cpm_s_avg as CPM_CPC,
  market_page_cpm_4search20.n as n_CPC
FROM (
    SELECT page_groupid_2, n_gtz
    FROM
      market_page_cpm_4search20_cpa
    ORDER BY n_gtz DESC
    LIMIT 10
  ) good_groupid_2 LEFT JOIN market_page_cpm_4search20_cpa
    ON good_groupid_2.page_groupid_2 = market_page_cpm_4search20_cpa.page_groupid_2  
  LEFT JOIN market_page_cpm_4search20
    ON good_groupid_2.page_groupid_2 = market_page_cpm_4search20.page_groupid_2