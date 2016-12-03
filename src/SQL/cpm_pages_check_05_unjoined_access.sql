set start_date='2016-11-14';
set end_date=  '2016-11-20';

WITH visits AS (
  SELECT DISTINCT user_id AS yandexuid
  FROM robot_market_logs.visits
  WHERE
    day BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date_visits} AND
    nvl(user_id, '') <> '' -- указан yandexuid
), access AS (
  SELECT DISTINCT yandexuid
  FROM robot_market_logs.front_access
  WHERE
    day BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date}
)

SELECT
  COUNT(*) as total_num_yandexuids, -- общее количество yandexuid
  SUM(IF(access.yandexuid IS NULL, 1, 0)) AS num_unjoined_yandexuids_from_visits, -- количество неподвязанных yandexuid к access
  SUM(IF(access.yandexuid IS NULL, 1, 0)) / COUNT(*) AS rate_from_visits,
  SUM(IF(visits.yandexuid IS NULL, 1, 0)) AS num_unjoined_yandexuids_from_access, -- количество неподвязанных yandexuid к access
  SUM(IF(visits.yandexuid IS NULL, 1, 0)) / COUNT(*) AS rate_from_access
FROM visits FULL JOIN access
  ON visits.yandexuid = access.yandexuid