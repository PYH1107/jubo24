SELECT
    *
FROM
    `jubo-ai.raw_dev_datahub_mongo.{{ table_name }}`
where
    lastModifiedDate < '{{ today }}'
LIMIT 0
