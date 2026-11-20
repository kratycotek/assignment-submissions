MERGE INTO DIM_CLASS AS t
USING (
    SELECT DISTINCT
        s.class_id,
        s.class_name
    FROM TMP_ITEM_MASTER AS s
    WHERE s.batch_id = '{{ params.batch_id }}'
      AND s.class_id IS NOT NULL
) AS s
    ON t.CLASS_ID = s.class_id
WHEN MATCHED AND t.CLASS_NAME <> s.class_name THEN
    UPDATE SET
        CLASS_NAME  = s.class_name,
        REC_UPDT_TS = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (CLASS_ID, CLASS_NAME, DEPARTMENT_KY, SRC_SYS_CD, BATCH_ID)
    VALUES (
        s.class_id,
        s.class_name,
        (SELECT DEPARTMENT_KY FROM DIM_DEPARTMENT WHERE DEPARTMENT_ID = 0),
        'ITEM_MSTR',
        '{{ params.batch_id }}'
    );
