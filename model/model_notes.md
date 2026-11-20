# Model Notes: RETAIL_DW

## Facts

| Fact | Grain | Type | Notes |
|---|---|---|---|
| FCT_SALES | item, location, day, transaction line | transaction | TRANSACTION_ID and TRANSACTION_LINE_NBR are degenerate dimensions: the same item can appear on two lines at different prices, so the line is the grain. CUSTOMER_KY is an FK but not in the PK; the customer belongs to the transaction, not the line. |
| FCT_PRICE | item, location, day | positional | One holding price per key. EFF_TO_DT = 9999-12-31 means currently in force; facts carry no CURR_FLG. HOLDING_PRICE_AMT is semi-additive. |
| FCT_INVENTORY | item, location, day | positional | End-of-day snapshot. ON_HAND_QTY and INVENTORY_VALUE_AMT are semi-additive; RECEIVED_QTY is fully additive. |
| AGG_SALES_WEEKLY | item, location, week | aggregate | Derived from FCT_SALES by ETL, never source-loaded. Fully additive measures plus SRC_ROW_CNT for reconciliation against the base. Loaded by a separate weekly DAG, not the daily one: a weekly total is only final once the week closes. |

## SCD

| Dimension | Attribute | SCD | Reason |
|---|---|---|---|
| DIM_ITEM | ITEM_NAME, CLASS_KY | SCD2 | Sales must be attributed to the name and class in force on the sale date |
| DIM_ITEM | BRAND_NAME | SCD1 | Cosmetic correction |
| DIM_LOCN | ADDRESS_TXT, DISTRICT_KY | SCD2 | Relocation and district reassignment change historical comparability and roll-up |
| DIM_LOCN | LOCN_NAME | SCD1 | Cosmetic rename |
| Product hierarchy above item | all | SCD1 | Corrections only |
| Org hierarchy above district | all | SCD1 | Corrections only |
| DIM_CUSTOMER | all | SCD1 | No historical segment attribution needed |
| Fiscal calendar (year, month, week, day) | all | SCD1 | Seeded reference data, immutable in practice |

## Write patterns

| Table class | Write | Why a rerun changes nothing |
|---|---|---|
| SCD2 dims (DIM_ITEM, DIM_LOCN) | Close the covering version (EFF_TO_DT = ds), insert the new one (EFF_FROM_DT = ds) | The new version already covers ds |
| SCD1 dims | MERGE on the business id | Every row matches and updates nothing |
| FCT_SALES | Delete the batch day, insert | The day is replaced |
| FCT_PRICE, FCT_INVENTORY | MERGE on (ITEM_KY, LOCN_KY, DAY_KY) | Today's rows are updated in place |
| AGG_SALES_WEEKLY | MERGE on (ITEM_KY, LOCN_KY, WEEK_KY) | The same weeks are recomputed |
| Control tables | MERGE, or delete per batch then insert | C_LOAD_DATES keeps the max date; rejects and params are cleared per batch first |

An SCD2 dimension is the one place "upsert" is not a merge on the natural key. Merging on ITEM_ID would overwrite history; close-then-open is what creates a new version instead.

## Hierarchies

- Product: DIM_ITEM > DIM_CLASS > DIM_DEPARTMENT > DIM_DIVISION > DIM_COMPANY
- Organisation: DIM_LOCN > DIM_DISTRICT > DIM_REGION > DIM_CHAIN > DIM_ORG_COMPANY
- Fiscal: DIM_DAY > DIM_FISCAL_WEEK > DIM_FISCAL_MONTH > DIM_FISCAL_YEAR

Each level FKs only to its immediate parent. No cross-level FKs.

## Maintenance columns

| Table class | REC_INS_TS | REC_UPDT_TS | SRC_SYS_CD | BATCH_ID | CURR_FLG | EFF_FROM_DT | EFF_TO_DT |
|---|---|---|---|---|---|---|---|
| SCD2 dims | yes | yes | yes | yes | yes | yes | yes |
| SCD1 dims | yes | yes | yes | yes | no | no | no |
| FCT_SALES | yes | yes | yes | yes | no | no | no |
| FCT_PRICE, FCT_INVENTORY | yes | yes | yes | yes | no | yes | yes |
| AGG_SALES_WEEKLY | yes | yes | yes | yes | no | yes | yes |

## Assumptions

- One price per item, location and day. A concurrent list/promo price would need DIM_PRICE_TYPE in the grain.
- Inventory is an end-of-day snapshot.
- One customer per transaction line; the customer is a property of the transaction.
- The fiscal calendar is seeded 2020-01-01 to 2030-12-31.
- The business reports weekly; daily reporting comes from FCT_SALES, the aggregate is derived, not a replacement.
