set start_date=      '2016-10-26';
set end_date=        '2016-11-24';



WITH visits AS (
  SELECT
    visit_id as visit_id,
    utc_start_time as visit_start_time,
    duration as visit_duration,
    user_id as yandexuid,
    INT(SUBSTR(user_id, 1, 7)) % ${hiveconf:n_groups} AS user_group
  FROM
    robot_market_logs.visits
  WHERE
    day BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date_visits} AND
    nvl(user_id, '') <> '' AND -- указан yandexuid
    -- список визитов получен из запроса cpm_pages_check_01_huge_cpm_01.sql
    -- это только топ
    visit_id IN ( 6676350804032751486,
                  6369656386470292132,
                  6367619087304239777,
                  6344555109134632827,
                  6355824268289509235,
                  6354358334624377515,
                  6862838556183758613)

)


SELECT yandexuid
FROM visits