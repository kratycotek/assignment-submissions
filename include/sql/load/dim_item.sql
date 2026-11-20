SELECT CAST(CASE
    WHEN NOT EXISTS (
        SELECT 1
        FROM TMP_ITEM_MASTER AS s
        JOIN DIM_CLASS AS c
          ON c.CLASS_ID = s.class_id
        LEFT JOIN DIM_ITEM AS v
          ON v.ITEM_ID = s.item_id
         AND DATE '{{ ds }}' >= v.EFF_FROM_DT
         AND DATE '{{ ds }}' <  v.EFF_TO_DT
        WHERE s.batch_id = '{{ params.batch_id }}'
          AND s.item_id IS NOT NULL
          AND (
              v.ITEM_KY IS NULL
              OR v.ITEM_NAME <> s.item_name
              OR v.CLASS_KY <> c.CLASS_KY
          )
    )
    OR DATE '{{ ds }}' >= (SELECT COALESCE(MAX(EFF_FROM_DT), DATE '1900-01-01') FROM DIM_ITEM)
    THEN '1'
    ELSE 'ORDER_VIOLATION: ds is earlier than the latest DIM_ITEM version and would change history'
END AS INTEGER) AS ORDER_GUARD;

UPDATE DIM_ITEM AS t
SET
    BRAND_NAME  = s.brand_name,
    REC_UPDT_TS = CURRENT_TIMESTAMP()
FROM TMP_ITEM_MASTER AS s
WHERE s.batch_id = '{{ params.batch_id }}'
  AND t.ITEM_ID = s.item_id
  AND COALESCE(t.BRAND_NAME, '') <> COALESCE(s.brand_name, '');

UPDATE DIM_ITEM AS t
SET
    CURR_FLG    = FALSE,
    EFF_TO_DT   = DATE '{{ ds }}',
    REC_UPDT_TS = CURRENT_TIMESTAMP()
WHERE DATE '{{ ds }}' >= t.EFF_FROM_DT
  AND DATE '{{ ds }}' <  t.EFF_TO_DT
  AND EXISTS (
      SELECT 1
      FROM TMP_ITEM_MASTER AS s
      JOIN DIM_CLASS AS c
        ON c.CLASS_ID = s.class_id
      WHERE s.batch_id = '{{ params.batch_id }}'
        AND s.item_id = t.ITEM_ID
        AND (s.item_name <> t.ITEM_NAME OR c.CLASS_KY <> t.CLASS_KY)
  );

INSERT INTO DIM_ITEM (
    ITEM_ID, ITEM_NAME, BRAND_NAME, CLASS_KY,
    SRC_SYS_CD, BATCH_ID, CURR_FLG, EFF_FROM_DT, EFF_TO_DT
)
SELECT DISTINCT
    s.item_id,
    s.item_name,
    s.brand_name,
    c.CLASS_KY,
    'ITEM_MSTR',
    '{{ params.batch_id }}',
    TRUE,
    DATE '{{ ds }}',
    DATE '9999-12-31'
FROM TMP_ITEM_MASTER AS s
JOIN DIM_CLASS AS c
  ON c.CLASS_ID = s.class_id
WHERE s.batch_id = '{{ params.batch_id }}'
  AND s.item_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM DIM_ITEM AS t
      WHERE t.ITEM_ID = s.item_id
        AND DATE '{{ ds }}' >= t.EFF_FROM_DT
        AND DATE '{{ ds }}' <  t.EFF_TO_DT
        AND t.ITEM_NAME = s.item_name
        AND t.CLASS_KY  = c.CLASS_KY
  );
