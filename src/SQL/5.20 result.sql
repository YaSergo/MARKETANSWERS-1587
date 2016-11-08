SELECT
  *
FROM
(
  SELECT
    *, 'CPC' as type
  FROM
    market_page_cpm_final

  UNION ALL

  SELECT
    *, 'CPA' as type
  FROM
    market_page_cpm_final_cpa
) t
WHERE n > 30