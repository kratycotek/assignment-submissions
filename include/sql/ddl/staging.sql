CREATE TABLE IF NOT EXISTS STG_ITEM_MASTER (
    item_id                 STRING,
    item_name               STRING,
    brand_name              STRING,
    class_id                STRING,
    class_name              STRING,
    BATCH_ID        STRING NOT NULL,
    SRC_FILE_NM     STRING,
    SRC_ROW_NUM     INTEGER,
    INGEST_TS       TIMESTAMP_NTZ,
    SRC_LOAD_TS     TIMESTAMP_NTZ,
    REC_INS_TS      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() NOT NULL
);

CREATE TABLE IF NOT EXISTS STG_STORE_MASTER (
    store_id                STRING,
    store_name              STRING,
    address_txt             STRING,
    district_id             STRING,
    district_name           STRING,
    BATCH_ID        STRING NOT NULL,
    SRC_FILE_NM     STRING,
    SRC_ROW_NUM     INTEGER,
    INGEST_TS       TIMESTAMP_NTZ,
    SRC_LOAD_TS     TIMESTAMP_NTZ,
    REC_INS_TS      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() NOT NULL
);

CREATE TABLE IF NOT EXISTS STG_POS_SALES (
    transaction_id          STRING,
    line_number             STRING,
    item_id                 STRING,
    store_id                STRING,
    sale_qty                STRING,
    sale_amount             STRING,
    BATCH_ID        STRING NOT NULL,
    SRC_FILE_NM     STRING,
    SRC_ROW_NUM     INTEGER,
    INGEST_TS       TIMESTAMP_NTZ,
    SRC_LOAD_TS     TIMESTAMP_NTZ,
    REC_INS_TS      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() NOT NULL
);

CREATE TABLE IF NOT EXISTS STG_INVENTORY (
    item_id                 STRING,
    store_id                STRING,
    on_hand_qty             STRING,
    inventory_value_amt     STRING,
    received_qty            STRING,
    BATCH_ID        STRING NOT NULL,
    SRC_FILE_NM     STRING,
    SRC_ROW_NUM     INTEGER,
    INGEST_TS       TIMESTAMP_NTZ,
    SRC_LOAD_TS     TIMESTAMP_NTZ,
    REC_INS_TS      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() NOT NULL
);

CREATE TABLE IF NOT EXISTS STG_PRICE (
    item_id                 STRING,
    store_id                STRING,
    holding_price_amt       STRING,
    BATCH_ID        STRING NOT NULL,
    SRC_FILE_NM     STRING,
    SRC_ROW_NUM     INTEGER,
    INGEST_TS       TIMESTAMP_NTZ,
    SRC_LOAD_TS     TIMESTAMP_NTZ,
    REC_INS_TS      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() NOT NULL
);
