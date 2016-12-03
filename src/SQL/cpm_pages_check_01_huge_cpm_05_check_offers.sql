set start_date=      '2016-10-26';
set end_date=        '2016-11-24';


SELECT *
FROM dictionaries.offers
WHERE day >= ${hiveconf:start_date}
AND binary_price_price > 100000000 -- 100 миллионов



SELECT *
FROM dictionaries.cpa_clicks
WHERE day >= ${hiveconf:start_date}
AND offer_price > 100000000 -- 100 миллионов


SELECT *
FROM dictionaries.offers
WHERE
  day BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date} AND
  binary_ware_md5 = 'BxhYfWwfFdyypaauQCY_NA'
