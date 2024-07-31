CREATE
OR REPLACE TABLE jubo-ai.`{{ bq_dataset }}.{{ table }}` as (
    SELECT
        *
    FROM
        EXTERNAL_QUERY(
            'projects/jubo-ai/locations/asia-east1/connections/jubo-ai-datahub-{{ pg_dataset }}',
            'SELECT * FROM "{{ table }}";'
        )
);