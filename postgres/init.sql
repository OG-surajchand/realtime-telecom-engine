CREATE TABLE IF NOT EXISTS orders (
    timestamp TIMESTAMP,
    order_id TEXT,
    user_id TEXT,
    amount NUMERIC(10, 2),
    status TEXT
);