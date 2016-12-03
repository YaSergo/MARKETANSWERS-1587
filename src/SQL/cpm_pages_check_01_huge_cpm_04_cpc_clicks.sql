set start_date=      '2016-10-26';
set end_date=        '2016-11-24';



WITH clicks_cpc AS (
  SELECT
    unix_timestamp(eventtime) AS click_time,
    cookie AS yandexuid,
    price*30/100 AS price  -- Цена клика в рублях
  FROM robot_market_logs.clicks
  WHERE
    day BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date} AND
    nvl(filter, 0) = 0 AND  -- убираю накрутку
    state = 1 AND -- убираю действия сотрудников яндекса
    cookie IN ( 1168890891461840279, --
                186007071475158691,  --
                6802133501476020529, --
                2634376391473287236, --
                4329379691477546306) --

)

SELECT *
FROM clicks_cpc
ORDER BY click_time