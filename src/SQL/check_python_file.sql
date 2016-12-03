delete FILES;
add FILE hdfs://marmot/user/medintsev/MARKETANSWERS-1587/src/python/bootstrap_cpm.py;

SELECT
  TRANSFORM ('1', '10395948', '5.88478017179', '35', '0.407445724956', '[6.5,5.7,5.5,5.7,5.6,5.5,5.8,5.4,5.6,5.7,5.0,5.7,6.0,6.3,5.9,6.4,5.8,6.4,6.2,5.9,6.4,6.1,6.2,5.5,5.9,5.8,6.6,6.2,6.3,5.6,5.2,5.8,5.8,5.3,6.5]')
  USING 'python bootstrap_cpm.py'
  AS   (
    page_groupid_1,
    page_groupid_2,
    cpm,
    n,
    sd,
    cpms,
    bootstrap_mean,
    left_border,
    right_border
  )