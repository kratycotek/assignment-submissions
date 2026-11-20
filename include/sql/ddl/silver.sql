CREATE TABLE IF NOT EXISTS TMP_ITEM_MASTER (
    item_id                 INTEGER,
    item_name               STRING,
    brand_name              STRING,
    class_id                INTEGER,
    class_name              STRING,
    BATCH_ID        STRING NOT NULL,
    SRC_FILE_NM     STRING,
    SRC_LOAD_TS     TIMESTAMP_NTZ,
    REC_INS_TS      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() NOT NULL
);

CREATE TABLE IF NOT EXISTS TMP_STORE_MASTER (
    store_id                INTEGER,
    store_name              STRING,
    address_txt             STRING,
    district_id             INTEGER,
    district_name           STRING,
    BATCH_ID        STRING NOT NULL,
    SRC_FILE_NM     STRING,
    SRC_LOAD_TS     TIMESTAMP_NTZ,
    REC_INS_TS      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() NOT NULL
);

CREATE TABLE IF NOT EXISTS TMP_POS_SALES (
    transaction_id          INTEGER,
    line_number             INTEGER,
    item_id                 INTEGER,
    store_id                INTEGER,
    sale_qty                INTEGER,
    sale_amount             DECIMAL(18,4),
    ITEM_KY                 INTEGER,
    LOCN_KY                 INTEGER,
    DAY_KY                  INTEGER,
    BATCH_ID        STRING NOT NULL,
    SRC_FILE_NM     STRING,
    SRC_LOAD_TS     TIMESTAMP_NTZ,
    REC_INS_TS      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() NOT NULL
);

CREATE TABLE IF NOT EXISTS TMP_INVENTORY (
    item_id                 INTEGER,
    store_id                INTEGER,
    on_hand_qty             INTEGER,
    inventory_value_amt     DECIMAL(18,4),
    received_qty            INTEGER,
    ITEM_KY                 INTEGER,
    LOCN_KY                 INTEGER,
    DAY_KY                  INTEGER,
    BATCH_ID        STRING NOT NULL,
    SRC_FILE_NM     STRING,
    SRC_LOAD_TS     TIMESTAMP_NTZ,
    REC_INS_TS      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() NOT NULL
);

CREATE TABLE IF NOT EXISTS TMP_PRICE (
    item_id                 INTEGER,
    store_id                INTEGER,
    holding_price_amt       DECIMAL(18,4),
    ITEM_KY                 INTEGER,
    LOCN_KY                 INTEGER,
    DAY_KY                  INTEGER,
    BATCH_ID        STRING NOT NULL,
    SRC_FILE_NM     STRING,
    SRC_LOAD_TS     TIMESTAMP_NTZ,
    REC_INS_TS      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() NOT NULL
);
