set start_date=      '2016-10-26';
set end_date=        '2016-11-24';



WITH clicks_cpa AS (
  SELECT
    unix_timestamp(eventtime) as click_time, -- время клика
    cookie as yandexuid, -- id пользователя
    offer_price*fee*0.05 AS price_my,  -- Цена клика в рублях с учетом CPA коэффициента
    -- 0.05 оценочное значение, после запуска https://paste.yandex-team.ru/170192
    -- https://st.yandex-team.ru/MARKETANSWERS-1587#1477662737000
    *
  FROM robot_market_logs.cpa_clicks
  WHERE
    day BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date} AND
    nvl(filter, 0) = 0 AND  -- убираю накрутку
    state = 1 AND -- убираю действия сотрудников яндекса
    nvl(type_id, 0) = 0 AND -- только нажатия на кнопку в корзину
    cookie IN ( 1168890891461840279,
                186007071475158691,
                6802133501476020529,
                2634376391473287236,
                4329379691477546306)

)

SELECT *
FROM clicks_cpa
ORDER BY click_time