MERGE INTO FCT_INVENTORY AS t
USING (
    SELECT
        s.ITEM_KY,
        s.LOCN_KY,
        s.DAY_KY,
        s.on_hand_qty,
        s.inventory_value_amt,
        s.received_qty
    FROM TMP_INVENTORY AS s
    WHERE s.BATCH_ID = '{{ params.batch_id }}'
) AS s
    ON  t.ITEM_KY = s.ITEM_KY
    AND t.LOCN_KY = s.LOCN_KY
    AND t.DAY_KY  = s.DAY_KY
WHEN MATCHED THEN
    UPDATE SET
        ON_HAND_QTY         = s.on_hand_qty,
        INVENTORY_VALUE_AMT = s.inventory_value_amt,
        RECEIVED_QTY        = s.received_qty,
        REC_UPDT_TS         = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        ITEM_KY, LOCN_KY, DAY_KY,
        ON_HAND_QTY, INVENTORY_VALUE_AMT, RECEIVED_QTY,
        SRC_SYS_CD, BATCH_ID, EFF_FROM_DT, EFF_TO_DT
    )
    VALUES (
        s.ITEM_KY, s.LOCN_KY, s.DAY_KY,
        s.on_hand_qty, s.inventory_value_amt, s.received_qty,
        'INV', '{{ params.batch_id }}', DATE '{{ ds }}', DATE '9999-12-31'
    );

MERGE INTO FCT_INVENTORY AS t
USING (
    SELECT
        f.ITEM_KY,
        f.LOCN_KY,
        f.DAY_KY,
        d.CALENDAR_DT AS NEW_FROM_DT,
        COALESCE(
            LEAD(d.CALENDAR_DT) OVER (PARTITION BY f.ITEM_KY, f.LOCN_KY ORDER BY d.CALENDAR_DT),
            DATE '9999-12-31'
        ) AS NEW_TO_DT
    FROM FCT_INVENTORY AS f
    JOIN DIM_DAY AS d
      ON d.DAY_KY = f.DAY_KY
) AS s
    ON  t.ITEM_KY = s.ITEM_KY
    AND t.LOCN_KY = s.LOCN_KY
    AND t.DAY_KY  = s.DAY_KY
WHEN MATCHED AND (t.EFF_FROM_DT <> s.NEW_FROM_DT OR t.EFF_TO_DT <> s.NEW_TO_DT) THEN
    UPDATE SET
        EFF_FROM_DT = s.NEW_FROM_DT,
        EFF_TO_DT   = s.NEW_TO_DT,
        REC_UPDT_TS = CURRENT_TIMESTAMP();
