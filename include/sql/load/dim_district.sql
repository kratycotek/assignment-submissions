MERGE INTO DIM_DISTRICT AS t
USING (
    SELECT DISTINCT
        s.district_id,
        s.district_name
    FROM TMP_STORE_MASTER AS s
    WHERE s.batch_id = '{{ params.batch_id }}'
      AND s.district_id IS NOT NULL
) AS s
    ON t.DISTRICT_ID = s.district_id
WHEN MATCHED AND t.DISTRICT_NAME <> s.district_name THEN
    UPDATE SET
        DISTRICT_NAME = s.district_name,
        REC_UPDT_TS   = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (DISTRICT_ID, DISTRICT_NAME, REGION_KY, SRC_SYS_CD, BATCH_ID)
    VALUES (
        s.district_id,
        s.district_name,
        (SELECT REGION_KY FROM DIM_REGION WHERE REGION_ID = 0),
        'STORE_MSTR',
        '{{ params.batch_id }}'
    );
