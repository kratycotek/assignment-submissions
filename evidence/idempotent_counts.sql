-- Rubric: "A second run for the same date changes no counts and no totals."
-- Run the BEFORE block, replay batch 20250103 in Airflow
--   (clear silver_fact|run_fact|reconcile for -s 2025-01-03 -e 2025-01-03),
-- then run the AFTER block. The two outputs must be identical.

USE WAREHOUSE AIRFLOW_WH;
USE DATABASE AIRFLOW_DB;
USE SCHEMA BOOTCAMP;

SELECT 'FCT_SALES'    AS TABLE_NM,
       COUNT(*)                AS ROW_CNT,
       SUM(NET_SALES_AMT)      AS TOTAL_AMT,
       SUM(QUANTITY_QTY)       AS QTY
FROM FCT_SALES
WHERE BATCH_ID = '20250103'
UNION ALL
SELECT 'FCT_PRICE',
       COUNT(*), SUM(HOLDING_PRICE_AMT), NULL
FROM FCT_PRICE
WHERE BATCH_ID = '20250103'
UNION ALL
SELECT 'FCT_INVENTORY',
       COUNT(*), SUM(ON_HAND_QTY), SUM(RECEIVED_QTY)
FROM FCT_INVENTORY
WHERE BATCH_ID = '20250103'
UNION ALL
SELECT 'C_REJ_TABLE',
       COUNT(*), NULL, NULL
FROM C_REJ_TABLE
WHERE BATCH_ID = '20250103'
UNION ALL
SELECT 'C_ALERT_TABLE',
       COUNT(*), NULL, NULL
FROM C_ALERT_TABLE
WHERE BATCH_ID = '20250103';

-- Expected per row: 51 / 12394.29 / 155; 50 / 8196.63 / null; 50 / 5848 / 273;
-- C_REJ_TABLE 5; C_ALERT_TABLE 1 (the day-3 reject-rate alert).
