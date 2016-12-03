delete FILES;
add FILE hdfs://marmot/user/medintsev/MARKETANSWERS-1587/src/python/bootstrap_cpm.py;

SELECT
  TRANSFORM (hyper_id, parents)
  USING 'python bootstrap_cpm.py'
  AS (hyper_id, parents)
FROM dictionaries.categories