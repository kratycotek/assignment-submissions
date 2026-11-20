DELETE FROM FCT_SALES
WHERE DAY_KY = (
    SELECT DAY_KY FROM DIM_DAY WHERE CALENDAR_DT = DATE '{{ ds }}'
);

INSERT INTO FCT_SALES (
    ITEM_KY, LOCN_KY, DAY_KY, CUSTOMER_KY,
    TRANSACTION_ID, TRANSACTION_LINE_NBR,
    NET_SALES_AMT, QUANTITY_QTY, SRC_SYS_CD, BATCH_ID
)
SELECT
    s.ITEM_KY,
    s.LOCN_KY,
    s.DAY_KY,
    c.CUSTOMER_KY,
    s.transaction_id,
    s.line_number,
    s.sale_amount,
    s.sale_qty,
    'POS',
    '{{ params.batch_id }}'
FROM TMP_POS_SALES AS s
JOIN DIM_CUSTOMER AS c
  ON c.CUSTOMER_ID = 0
WHERE s.BATCH_ID = '{{ params.batch_id }}';
