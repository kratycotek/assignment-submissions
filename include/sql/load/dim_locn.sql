SELECT CAST(CASE
    WHEN NOT EXISTS (
        SELECT 1
        FROM TMP_STORE_MASTER AS s
        JOIN DIM_DISTRICT AS d
          ON d.DISTRICT_ID = s.district_id
        LEFT JOIN DIM_LOCN AS v
          ON v.LOCN_ID = s.store_id
         AND DATE '{{ ds }}' >= v.EFF_FROM_DT
         AND DATE '{{ ds }}' <  v.EFF_TO_DT
        WHERE s.batch_id = '{{ params.batch_id }}'
          AND s.store_id IS NOT NULL
          AND (
              v.LOCN_KY IS NULL
              OR COALESCE(v.ADDRESS_TXT, '') <> COALESCE(s.address_txt, '')
              OR v.DISTRICT_KY <> d.DISTRICT_KY
          )
    )
    OR DATE '{{ ds }}' >= (SELECT COALESCE(MAX(EFF_FROM_DT), DATE '1900-01-01') FROM DIM_LOCN)
    THEN '1'
    ELSE 'ORDER_VIOLATION: ds is earlier than the latest DIM_LOCN version and would change history'
END AS INTEGER) AS ORDER_GUARD;

UPDATE DIM_LOCN AS t
SET
    LOCN_NAME   = s.store_name,
    REC_UPDT_TS = CURRENT_TIMESTAMP()
FROM TMP_STORE_MASTER AS s
WHERE s.batch_id = '{{ params.batch_id }}'
  AND t.LOCN_ID = s.store_id
  AND COALESCE(t.LOCN_NAME, '') <> COALESCE(s.store_name, '');

UPDATE DIM_LOCN AS t
SET
    CURR_FLG    = FALSE,
    EFF_TO_DT   = DATE '{{ ds }}',
    REC_UPDT_TS = CURRENT_TIMESTAMP()
WHERE DATE '{{ ds }}' >= t.EFF_FROM_DT
  AND DATE '{{ ds }}' <  t.EFF_TO_DT
  AND EXISTS (
      SELECT 1
      FROM TMP_STORE_MASTER AS s
      JOIN DIM_DISTRICT AS d
        ON d.DISTRICT_ID = s.district_id
      WHERE s.batch_id = '{{ params.batch_id }}'
        AND s.store_id = t.LOCN_ID
        AND (
            COALESCE(s.address_txt, '') <> COALESCE(t.ADDRESS_TXT, '')
            OR d.DISTRICT_KY <> t.DISTRICT_KY
        )
  );

INSERT INTO DIM_LOCN (
    LOCN_ID, LOCN_NAME, ADDRESS_TXT, DISTRICT_KY,
    SRC_SYS_CD, BATCH_ID, CURR_FLG, EFF_FROM_DT, EFF_TO_DT
)
SELECT DISTINCT
    s.store_id,
    s.store_name,
    s.address_txt,
    d.DISTRICT_KY,
    'STORE_MSTR',
    '{{ params.batch_id }}',
    TRUE,
    DATE '{{ ds }}',
    DATE '9999-12-31'
FROM TMP_STORE_MASTER AS s
JOIN DIM_DISTRICT AS d
  ON d.DISTRICT_ID = s.district_id
WHERE s.batch_id = '{{ params.batch_id }}'
  AND s.store_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM DIM_LOCN AS t
      WHERE t.LOCN_ID = s.store_id
        AND DATE '{{ ds }}' >= t.EFF_FROM_DT
        AND DATE '{{ ds }}' <  t.EFF_TO_DT
        AND COALESCE(t.ADDRESS_TXT, '') = COALESCE(s.address_txt, '')
        AND t.DISTRICT_KY = d.DISTRICT_KY
  );
