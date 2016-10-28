DROP TABLE IF EXISTS medintsev.market_page_cpm_simple;
CREATE TABLE medintsev.market_page_cpm_simple
  COMMENT 'CPM simple - cpm рассчитанное по упрощённой формуле'
AS
SELECT
  access.hyper_id as hyper_id,
  clicks_cpc.cpc as cpc,            -- суммарно по CPC / фишка-центы
  clicks_cpc.n_cpc as n_cpc,        -- количество CPC кликов
  clicks_cpa.cpa as cpa,            -- суммарно по CPA / фишка-центы
  clicks_cpa.n_cpa as n_cpa,        -- количество CPA кликов
  access.num_access as num_access,  -- количество показов
  access.num_yandexuid as num_yandexuid,
  (nvl(clicks_cpc.cpc, 0) + nvl(clicks_cpa.cpa, 0) * 0.05) / access.num_access as cpm
FROM
(
  SELECT page_groupid_2 as hyper_id, count(*) as num_access, count(DISTINCT yandexuid) as num_yandexuid
  FROM medintsev.market_page_cpm_access20
  WHERE
    page_groupid_1 = 1 -- Карточки моделей
  GROUP BY page_groupid_2
) access FULL JOIN (
  SELECT hyper_id, sum(price) as cpc, count(*) as n_cpc
  FROM medintsev.market_page_cpm_clicks20
  WHERE
    hyper_id <> -1 AND
    pp IN (6, 61, 62, 63, 64, 13, 21, 200, 201, 205, 206, 207, 208, 209, 210, 211, 26, 27, 144)
  GROUP BY hyper_id
) clicks_cpc
ON access.hyper_id = clicks_cpc.hyper_id
FULL JOIN (
  SELECT hyper_id, sum(original_price)/30*100 as cpa, count(*) as n_cpa
  FROM medintsev.market_page_cpm_clicks20_cpa
  WHERE
    hyper_id <> -1 AND
    pp IN (6, 61, 62, 63, 64, 13, 21, 200, 201, 205, 206, 207, 208, 209, 210, 211, 26, 27, 144)
  GROUP BY hyper_id
) clicks_cpa
ON access.hyper_id = clicks_cpa.hyper_id