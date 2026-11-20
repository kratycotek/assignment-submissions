-- 33 reconciliation checks, evaluated by medallion/reconcile.py.
-- Every row: CHECK_NM, CHECK_TYPE (EQ must equal, LE must stay <=), LEFT_VALUE, RIGHT_VALUE.
-- Values are text; compare as Decimal for EQ/LE, string for dates.
--
-- Table names come from tables.yaml; thresholds and sources from pipeline.json.

-- item_master: file rows = staging + Q2 rejects
SELECT 'bronze_item_master_rows' AS CHECK_NM,
    'EQ' AS CHECK_TYPE,
    TO_VARCHAR(
      {{ source.item_master_rows }}
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM {{ tables.stg.item_master }} WHERE BATCH_ID = '{{ params.batch_id }}')
      + (SELECT COUNT(*) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'BRONZE' AND SRC_TABLE_NM = '{{ tables.stg.item_master }}' AND GATE_ID = 'Q2')
    ) AS RIGHT_VALUE
UNION ALL

-- store_master: file rows = staging + Q2 rejects
SELECT 'bronze_store_master_rows' AS CHECK_NM,
    'EQ' AS CHECK_TYPE,
    TO_VARCHAR(
      {{ source.store_master_rows }}
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM {{ tables.stg.store_master }} WHERE BATCH_ID = '{{ params.batch_id }}')
      + (SELECT COUNT(*) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'BRONZE' AND SRC_TABLE_NM = '{{ tables.stg.store_master }}' AND GATE_ID = 'Q2')
    ) AS RIGHT_VALUE
UNION ALL

-- pos_sales: file rows = staging + Q2 rejects
SELECT 'bronze_pos_sales_rows' AS CHECK_NM,
    'EQ' AS CHECK_TYPE,
    TO_VARCHAR(
      {{ source.pos_sales_rows }}
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM {{ tables.stg.pos_sales }} WHERE BATCH_ID = '{{ params.batch_id }}')
      + (SELECT COUNT(*) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'BRONZE' AND SRC_TABLE_NM = '{{ tables.stg.pos_sales }}' AND GATE_ID = 'Q2')
    ) AS RIGHT_VALUE
UNION ALL

-- inventory: file rows = staging + Q2 rejects
SELECT 'bronze_inventory_rows' AS CHECK_NM,
    'EQ' AS CHECK_TYPE,
    TO_VARCHAR(
      {{ source.inventory_rows }}
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM {{ tables.stg.inventory }} WHERE BATCH_ID = '{{ params.batch_id }}')
      + (SELECT COUNT(*) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'BRONZE' AND SRC_TABLE_NM = '{{ tables.stg.inventory }}' AND GATE_ID = 'Q2')
    ) AS RIGHT_VALUE
UNION ALL

-- price: file rows = staging + Q2 rejects
SELECT 'bronze_price_rows' AS CHECK_NM,
    'EQ' AS CHECK_TYPE,
    TO_VARCHAR(
      {{ source.price_rows }}
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM {{ tables.stg.price }} WHERE BATCH_ID = '{{ params.batch_id }}')
      + (SELECT COUNT(*) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'BRONZE' AND SRC_TABLE_NM = '{{ tables.stg.price }}' AND GATE_ID = 'Q2')
    ) AS RIGHT_VALUE
UNION ALL

-- item_master: staging = silver + silver rejects
SELECT 'silver_item_master_rows' AS CHECK_NM,
    'EQ' AS CHECK_TYPE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM {{ tables.stg.item_master }} WHERE BATCH_ID = '{{ params.batch_id }}')
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM {{ tables.tmp.item_master }} WHERE BATCH_ID = '{{ params.batch_id }}')
      + (SELECT COUNT(*) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'SILVER' AND SRC_TABLE_NM = '{{ tables.stg.item_master }}')
    ) AS RIGHT_VALUE
UNION ALL

-- store_master: staging = silver + silver rejects
SELECT 'silver_store_master_rows' AS CHECK_NM,
    'EQ' AS CHECK_TYPE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM {{ tables.stg.store_master }} WHERE BATCH_ID = '{{ params.batch_id }}')
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM {{ tables.tmp.store_master }} WHERE BATCH_ID = '{{ params.batch_id }}')
      + (SELECT COUNT(*) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'SILVER' AND SRC_TABLE_NM = '{{ tables.stg.store_master }}')
    ) AS RIGHT_VALUE
UNION ALL

-- pos_sales: staging = silver + silver rejects
SELECT 'silver_pos_sales_rows' AS CHECK_NM,
    'EQ' AS CHECK_TYPE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM {{ tables.stg.pos_sales }} WHERE BATCH_ID = '{{ params.batch_id }}')
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM {{ tables.tmp.pos_sales }} WHERE BATCH_ID = '{{ params.batch_id }}')
      + (SELECT COUNT(*) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'SILVER' AND SRC_TABLE_NM = '{{ tables.stg.pos_sales }}')
    ) AS RIGHT_VALUE
UNION ALL

-- inventory: staging = silver + silver rejects
SELECT 'silver_inventory_rows' AS CHECK_NM,
    'EQ' AS CHECK_TYPE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM {{ tables.stg.inventory }} WHERE BATCH_ID = '{{ params.batch_id }}')
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM {{ tables.tmp.inventory }} WHERE BATCH_ID = '{{ params.batch_id }}')
      + (SELECT COUNT(*) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'SILVER' AND SRC_TABLE_NM = '{{ tables.stg.inventory }}')
    ) AS RIGHT_VALUE
UNION ALL

-- price: staging = silver + silver rejects
SELECT 'silver_price_rows' AS CHECK_NM,
    'EQ' AS CHECK_TYPE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM {{ tables.stg.price }} WHERE BATCH_ID = '{{ params.batch_id }}')
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM {{ tables.tmp.price }} WHERE BATCH_ID = '{{ params.batch_id }}')
      + (SELECT COUNT(*) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'SILVER' AND SRC_TABLE_NM = '{{ tables.stg.price }}')
    ) AS RIGHT_VALUE
UNION ALL

-- file sale amount = staging sale amount
SELECT 'bronze_pos_amount' AS CHECK_NM,
    'EQ' AS CHECK_TYPE,
    TO_VARCHAR(
      {{ source.pos_amount }}
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      (SELECT COALESCE(SUM(TRY_TO_DECIMAL(sale_amount, 18, 4)), 0) FROM {{ tables.stg.pos_sales }} WHERE BATCH_ID = '{{ params.batch_id }}')
    ) AS RIGHT_VALUE
UNION ALL

-- staging = silver + rejected amounts
SELECT 'silver_pos_amount' AS CHECK_NM,
    'EQ' AS CHECK_TYPE,
    TO_VARCHAR(
      (SELECT COALESCE(SUM(TRY_TO_DECIMAL(sale_amount, 18, 4)), 0) FROM {{ tables.stg.pos_sales }} WHERE BATCH_ID = '{{ params.batch_id }}')
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      (SELECT COALESCE(SUM(sale_amount), 0) FROM {{ tables.tmp.pos_sales }} WHERE BATCH_ID = '{{ params.batch_id }}')
      + (SELECT COALESCE(SUM(TRY_TO_DECIMAL(PAYLOAD:sale_amount::STRING, 18, 4)), 0) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'SILVER' AND SRC_TABLE_NM = '{{ tables.stg.pos_sales }}')
    ) AS RIGHT_VALUE
UNION ALL

-- silver rows - Q10 rejects = fact rows for the day
SELECT 'gold_sales_rows' AS CHECK_NM,
    'EQ' AS CHECK_TYPE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM {{ tables.tmp.pos_sales }} WHERE BATCH_ID = '{{ params.batch_id }}') - (SELECT COUNT(*) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'GOLD' AND SRC_TABLE_NM = '{{ tables.tmp.pos_sales }}' AND GATE_ID = 'Q10')
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM {{ tables.fct.sales }} WHERE DAY_KY = (SELECT DAY_KY FROM {{ tables.dim.day }} WHERE CALENDAR_DT = DATE '{{ ds }}'))
    ) AS RIGHT_VALUE
UNION ALL

-- silver amount - Q10 rejected amounts = fact net sales
SELECT 'gold_sales_amount' AS CHECK_NM,
    'EQ' AS CHECK_TYPE,
    TO_VARCHAR(
      (SELECT COALESCE(SUM(sale_amount), 0) FROM {{ tables.tmp.pos_sales }} WHERE BATCH_ID = '{{ params.batch_id }}') - (SELECT COALESCE(SUM(TRY_TO_DECIMAL(PAYLOAD:sale_amount::STRING, 18, 4)), 0) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'GOLD' AND SRC_TABLE_NM = '{{ tables.tmp.pos_sales }}')
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      (SELECT COALESCE(SUM(NET_SALES_AMT), 0) FROM {{ tables.fct.sales }} WHERE DAY_KY = (SELECT DAY_KY FROM {{ tables.dim.day }} WHERE CALENDAR_DT = DATE '{{ ds }}'))
    ) AS RIGHT_VALUE
UNION ALL

-- direct tie: file amount - all rejected amounts = fact net sales
SELECT 'gold_sales_vs_source' AS CHECK_NM,
    'EQ' AS CHECK_TYPE,
    TO_VARCHAR(
      {{ source.pos_amount }} - (SELECT COALESCE(SUM(TRY_TO_DECIMAL(PAYLOAD:sale_amount::STRING, 18, 4)), 0) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'SILVER' AND SRC_TABLE_NM = '{{ tables.stg.pos_sales }}') - (SELECT COALESCE(SUM(TRY_TO_DECIMAL(PAYLOAD:sale_amount::STRING, 18, 4)), 0) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'GOLD' AND SRC_TABLE_NM = '{{ tables.tmp.pos_sales }}')
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      (SELECT COALESCE(SUM(NET_SALES_AMT), 0) FROM {{ tables.fct.sales }} WHERE DAY_KY = (SELECT DAY_KY FROM {{ tables.dim.day }} WHERE CALENDAR_DT = DATE '{{ ds }}'))
    ) AS RIGHT_VALUE
UNION ALL

-- price: silver - Q10 rejects = fact rows for the day
SELECT 'gold_price_rows' AS CHECK_NM,
    'EQ' AS CHECK_TYPE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM {{ tables.tmp.price }} WHERE BATCH_ID = '{{ params.batch_id }}') - (SELECT COUNT(*) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'GOLD' AND SRC_TABLE_NM = '{{ tables.tmp.price }}' AND GATE_ID = 'Q10')
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM {{ tables.fct.price }} WHERE DAY_KY = (SELECT DAY_KY FROM {{ tables.dim.day }} WHERE CALENDAR_DT = DATE '{{ ds }}'))
    ) AS RIGHT_VALUE
UNION ALL

-- inventory: silver - Q10 rejects = fact rows for the day
SELECT 'gold_inventory_rows' AS CHECK_NM,
    'EQ' AS CHECK_TYPE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM {{ tables.tmp.inventory }} WHERE BATCH_ID = '{{ params.batch_id }}') - (SELECT COUNT(*) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'GOLD' AND SRC_TABLE_NM = '{{ tables.tmp.inventory }}' AND GATE_ID = 'Q10')
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM {{ tables.fct.inventory }} WHERE DAY_KY = (SELECT DAY_KY FROM {{ tables.dim.day }} WHERE CALENDAR_DT = DATE '{{ ds }}'))
    ) AS RIGHT_VALUE
UNION ALL

-- facts with key = 0 stay under threshold
SELECT 'unknown_fact_keys' AS CHECK_NM,
    'LE' AS CHECK_TYPE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM {{ tables.fct.sales }} WHERE DAY_KY = (SELECT DAY_KY FROM {{ tables.dim.day }} WHERE CALENDAR_DT = DATE '{{ ds }}') AND (ITEM_KY = 0 OR LOCN_KY = 0 OR DAY_KY = 0))
      + (SELECT COUNT(*) FROM {{ tables.fct.price }} WHERE DAY_KY = (SELECT DAY_KY FROM {{ tables.dim.day }} WHERE CALENDAR_DT = DATE '{{ ds }}') AND (ITEM_KY = 0 OR LOCN_KY = 0 OR DAY_KY = 0)) + (SELECT COUNT(*) FROM {{ tables.fct.inventory }} WHERE DAY_KY = (SELECT DAY_KY FROM {{ tables.dim.day }} WHERE CALENDAR_DT = DATE '{{ ds }}') AND (ITEM_KY = 0 OR LOCN_KY = 0 OR DAY_KY = 0))
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      {{ thresholds.unknown_fact_keys }}
    ) AS RIGHT_VALUE
UNION ALL

-- latest sales fact date = run date
SELECT 'latest_sales_date' AS CHECK_NM,
    'EQ' AS CHECK_TYPE,
    TO_VARCHAR(
      (SELECT MAX(d.CALENDAR_DT) FROM {{ tables.fct.sales }} AS f JOIN {{ tables.dim.day }} AS d ON d.DAY_KY = f.DAY_KY WHERE f.DAY_KY = (SELECT DAY_KY FROM {{ tables.dim.day }} WHERE CALENDAR_DT = DATE '{{ ds }}'))
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      DATE '{{ ds }}'
    ) AS RIGHT_VALUE
UNION ALL

-- latest price fact date = run date
SELECT 'latest_price_date' AS CHECK_NM,
    'EQ' AS CHECK_TYPE,
    TO_VARCHAR(
      (SELECT MAX(d.CALENDAR_DT) FROM {{ tables.fct.price }} AS f JOIN {{ tables.dim.day }} AS d ON d.DAY_KY = f.DAY_KY WHERE f.DAY_KY = (SELECT DAY_KY FROM {{ tables.dim.day }} WHERE CALENDAR_DT = DATE '{{ ds }}'))
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      DATE '{{ ds }}'
    ) AS RIGHT_VALUE
UNION ALL

-- latest inventory fact date = run date
SELECT 'latest_inventory_date' AS CHECK_NM,
    'EQ' AS CHECK_TYPE,
    TO_VARCHAR(
      (SELECT MAX(d.CALENDAR_DT) FROM {{ tables.fct.inventory }} AS f JOIN {{ tables.dim.day }} AS d ON d.DAY_KY = f.DAY_KY WHERE f.DAY_KY = (SELECT DAY_KY FROM {{ tables.dim.day }} WHERE CALENDAR_DT = DATE '{{ ds }}'))
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      DATE '{{ ds }}'
    ) AS RIGHT_VALUE
UNION ALL

-- sales: rows - distinct rows on fact grain = 0
SELECT 'fact_grain_dups_sales' AS CHECK_NM,
    'EQ' AS CHECK_TYPE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM {{ tables.fct.sales }} WHERE DAY_KY = (SELECT DAY_KY FROM {{ tables.dim.day }} WHERE CALENDAR_DT = DATE '{{ ds }}')) - (SELECT COUNT(*) FROM (SELECT DISTINCT ITEM_KY, LOCN_KY, DAY_KY, TRANSACTION_ID, TRANSACTION_LINE_NBR FROM {{ tables.fct.sales }} WHERE DAY_KY = (SELECT DAY_KY FROM {{ tables.dim.day }} WHERE CALENDAR_DT = DATE '{{ ds }}')))
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      0
    ) AS RIGHT_VALUE
UNION ALL

-- price: rows - distinct rows on fact grain = 0
SELECT 'fact_grain_dups_price' AS CHECK_NM,
    'EQ' AS CHECK_TYPE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM {{ tables.fct.price }} WHERE DAY_KY = (SELECT DAY_KY FROM {{ tables.dim.day }} WHERE CALENDAR_DT = DATE '{{ ds }}')) - (SELECT COUNT(*) FROM (SELECT DISTINCT ITEM_KY, LOCN_KY, DAY_KY FROM {{ tables.fct.price }} WHERE DAY_KY = (SELECT DAY_KY FROM {{ tables.dim.day }} WHERE CALENDAR_DT = DATE '{{ ds }}')))
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      0
    ) AS RIGHT_VALUE
UNION ALL

-- inventory: rows - distinct rows on fact grain = 0
SELECT 'fact_grain_dups_inventory' AS CHECK_NM,
    'EQ' AS CHECK_TYPE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM {{ tables.fct.inventory }} WHERE DAY_KY = (SELECT DAY_KY FROM {{ tables.dim.day }} WHERE CALENDAR_DT = DATE '{{ ds }}')) - (SELECT COUNT(*) FROM (SELECT DISTINCT ITEM_KY, LOCN_KY, DAY_KY FROM {{ tables.fct.inventory }} WHERE DAY_KY = (SELECT DAY_KY FROM {{ tables.dim.day }} WHERE CALENDAR_DT = DATE '{{ ds }}')))
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      0
    ) AS RIGHT_VALUE
UNION ALL

-- no two versions of one item/locn cover the same date
SELECT 'dim_window_overlap' AS CHECK_NM,
    'EQ' AS CHECK_TYPE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM {{ tables.dim.item }} AS a JOIN {{ tables.dim.item }} AS b ON a.ITEM_ID = b.ITEM_ID AND a.ITEM_KY < b.ITEM_KY AND a.EFF_FROM_DT < b.EFF_TO_DT AND b.EFF_FROM_DT < a.EFF_TO_DT)
      + (SELECT COUNT(*) FROM {{ tables.dim.locn }} AS a JOIN {{ tables.dim.locn }} AS b ON a.LOCN_ID = b.LOCN_ID AND a.LOCN_KY < b.LOCN_KY AND a.EFF_FROM_DT < b.EFF_TO_DT AND b.EFF_FROM_DT < a.EFF_TO_DT)
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      0
    ) AS RIGHT_VALUE
UNION ALL

-- EFF_FROM_DT < EFF_TO_DT for every version
SELECT 'dim_window_bounds' AS CHECK_NM,
    'EQ' AS CHECK_TYPE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM {{ tables.dim.item }} WHERE EFF_FROM_DT >= EFF_TO_DT)
      + (SELECT COUNT(*) FROM {{ tables.dim.locn }} WHERE EFF_FROM_DT >= EFF_TO_DT)
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      0
    ) AS RIGHT_VALUE
UNION ALL

-- exactly one CURR_FLG row per item/locn
SELECT 'dim_current_versions' AS CHECK_NM,
    'EQ' AS CHECK_TYPE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM (SELECT ITEM_ID FROM {{ tables.dim.item }} GROUP BY ITEM_ID HAVING SUM(CASE WHEN CURR_FLG THEN 1 ELSE 0 END) <> 1) AS g_I)
      + (SELECT COUNT(*) FROM (SELECT LOCN_ID FROM {{ tables.dim.locn }} GROUP BY LOCN_ID HAVING SUM(CASE WHEN CURR_FLG THEN 1 ELSE 0 END) <> 1) AS g_L)
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      0
    ) AS RIGHT_VALUE
UNION ALL

-- no gap between consecutive versions
SELECT 'dim_window_gaps' AS CHECK_NM,
    'EQ' AS CHECK_TYPE,
    TO_VARCHAR(
      (SELECT COUNT(*) FROM (SELECT EFF_TO_DT, LEAD(EFF_FROM_DT) OVER (PARTITION BY ITEM_ID ORDER BY EFF_FROM_DT) AS next_from FROM {{ tables.dim.item }}) AS w_I WHERE next_from IS NOT NULL AND EFF_TO_DT <> next_from)
      + (SELECT COUNT(*) FROM (SELECT EFF_TO_DT, LEAD(EFF_FROM_DT) OVER (PARTITION BY LOCN_ID ORDER BY EFF_FROM_DT) AS next_from FROM {{ tables.dim.locn }}) AS w_L WHERE next_from IS NOT NULL AND EFF_TO_DT <> next_from)
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      0
    ) AS RIGHT_VALUE
UNION ALL

-- item_master: rejected rows over source rows, percent
SELECT 'reject_rate_item_master' AS CHECK_NM,
    'LE' AS CHECK_TYPE,
    TO_VARCHAR(
      CASE WHEN {{ source.item_master_rows }} = 0 THEN 0 ELSE ROUND(100.0 * ((SELECT COUNT(*) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'BRONZE' AND SRC_TABLE_NM = '{{ tables.stg.item_master }}' AND GATE_ID = 'Q2')
      + (SELECT COUNT(*) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'SILVER' AND SRC_TABLE_NM = '{{ tables.stg.item_master }}')) / {{ source.item_master_rows }}, 4) END
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      {{ thresholds.reject_rate_pct }}
    ) AS RIGHT_VALUE
UNION ALL

-- store_master: rejected rows over source rows, percent
SELECT 'reject_rate_store_master' AS CHECK_NM,
    'LE' AS CHECK_TYPE,
    TO_VARCHAR(
      CASE WHEN {{ source.store_master_rows }} = 0 THEN 0 ELSE ROUND(100.0 * ((SELECT COUNT(*) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'BRONZE' AND SRC_TABLE_NM = '{{ tables.stg.store_master }}' AND GATE_ID = 'Q2')
      + (SELECT COUNT(*) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'SILVER' AND SRC_TABLE_NM = '{{ tables.stg.store_master }}')) / {{ source.store_master_rows }}, 4) END
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      {{ thresholds.reject_rate_pct }}
    ) AS RIGHT_VALUE
UNION ALL

-- pos_sales: rejected rows over source rows, percent
SELECT 'reject_rate_pos_sales' AS CHECK_NM,
    'LE' AS CHECK_TYPE,
    TO_VARCHAR(
      CASE WHEN {{ source.pos_sales_rows }} = 0 THEN 0 ELSE ROUND(100.0 * ((SELECT COUNT(*) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'BRONZE' AND SRC_TABLE_NM = '{{ tables.stg.pos_sales }}' AND GATE_ID = 'Q2')
      + (SELECT COUNT(*) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'SILVER' AND SRC_TABLE_NM = '{{ tables.stg.pos_sales }}') + (SELECT COUNT(*) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'GOLD' AND SRC_TABLE_NM = '{{ tables.tmp.pos_sales }}' AND GATE_ID = 'Q10')) / {{ source.pos_sales_rows }}, 4) END
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      {{ thresholds.reject_rate_pct }}
    ) AS RIGHT_VALUE
UNION ALL

-- inventory: rejected rows over source rows, percent
SELECT 'reject_rate_inventory' AS CHECK_NM,
    'LE' AS CHECK_TYPE,
    TO_VARCHAR(
      CASE WHEN {{ source.inventory_rows }} = 0 THEN 0 ELSE ROUND(100.0 * ((SELECT COUNT(*) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'BRONZE' AND SRC_TABLE_NM = '{{ tables.stg.inventory }}' AND GATE_ID = 'Q2')
      + (SELECT COUNT(*) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'SILVER' AND SRC_TABLE_NM = '{{ tables.stg.inventory }}') + (SELECT COUNT(*) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'GOLD' AND SRC_TABLE_NM = '{{ tables.tmp.inventory }}' AND GATE_ID = 'Q10')) / {{ source.inventory_rows }}, 4) END
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      {{ thresholds.reject_rate_pct }}
    ) AS RIGHT_VALUE
UNION ALL

-- price: rejected rows over source rows, percent
SELECT 'reject_rate_price' AS CHECK_NM,
    'LE' AS CHECK_TYPE,
    TO_VARCHAR(
      CASE WHEN {{ source.price_rows }} = 0 THEN 0 ELSE ROUND(100.0 * ((SELECT COUNT(*) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'BRONZE' AND SRC_TABLE_NM = '{{ tables.stg.price }}' AND GATE_ID = 'Q2')
      + (SELECT COUNT(*) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'SILVER' AND SRC_TABLE_NM = '{{ tables.stg.price }}') + (SELECT COUNT(*) FROM {{ tables.control.rej_table }} WHERE BATCH_ID = '{{ params.batch_id }}' AND FAILED_LAYER_CD = 'GOLD' AND SRC_TABLE_NM = '{{ tables.tmp.price }}' AND GATE_ID = 'Q10')) / {{ source.price_rows }}, 4) END
    ) AS LEFT_VALUE,
    TO_VARCHAR(
      {{ thresholds.reject_rate_pct }}
    ) AS RIGHT_VALUE
