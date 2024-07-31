SELECT
    time_start,
    time_end
FROM
    `jubo-ai.app_prod_knowledgegraph.focusKG_mapping`
ORDER BY
    time_end DESC
LIMIT
    1