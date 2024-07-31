DECLARE end_date DATETIME DEFAULT (SELECT DATETIME(CURRENT_DATE(), "16:00:00"));

INSERT INTO
     `jubo-ai.meta_prod_vitalsign.vitalsigns` (
             _id,
             patient,
             organization,
             createdDate,
             SYS,
             DIA,
             PR,
             TP,
             SPO2,
             RR,
             PAIN
     )

SELECT * FROM (
    SELECT
            SAFE_CAST(_id as string) as _id,
            patient,
            organization,
            SAFE_CAST(createdDate as datetime) as createdDate,
            SAFE_CAST(SYS as float64) as SYS,
            SAFE_CAST(DIA as float64) as DIA,
            SAFE_CAST(PR as float64) as PR,
            SAFE_CAST(TP as float64) as TP,
            SAFE_CAST(SPO2 as float64) as SPO2,
            SAFE_CAST(RR as float64) as RR,
            SAFE_CAST(PAIN as float64) as PAIN,
    FROM
            `jubo-ai.raw_prod_datahub_mongo.vitalsigns`
    WHERE
            _id NOT IN (
                    SELECT
                            _id
                    FROM
                            `jubo-ai.meta_prod_vitalsign.vitalsigns`
            )
) tmp WHERE createdDate >= DATE_SUB(end_date, INTERVAL 1 DAY)
        AND createdDate < end_date
