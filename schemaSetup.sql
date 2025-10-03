CREATE TABLE prices (
    id INTEGER PRIMARY KEY,
    crypto_id TEXT NOT NULL,
    price_usd REAL NOT NULL,
    ingestion_timestamp TEXT NOT NULL
);