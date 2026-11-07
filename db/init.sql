CREATE TABLE products (
  id INT PRIMARY KEY, name TEXT NOT NULL, price INT NOT NULL,
  stock INT NOT NULL CHECK (stock >= 0), initial_stock INT NOT NULL
);
INSERT INTO products VALUES
 (1,'Laptop',2400000,5,5),(2,'Smartphone',900000,10,10),(3,'Headphones',120000,20,20),
 (4,'Backpack',85000,15,15),(5,'Sneakers',150000,8,8),(6,'Tablet',1100000,6,6),
 (7,'Smartwatch',350000,12,12),(8,'Power Bank',60000,25,25),
 (99,'Load Test Item',1,1000000,1000000);

CREATE TABLE orders (
  id SERIAL PRIMARY KEY,
  client_request_id TEXT NOT NULL,
  customer TEXT, product_id INT, quantity INT NOT NULL, amount NUMERIC,
  status TEXT NOT NULL DEFAULT 'PLACED',
  version INT NOT NULL DEFAULT 1,
  created_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE idempotency_keys (
  key TEXT PRIMARY KEY, request_hash TEXT, order_id INT, created_at TIMESTAMPTZ DEFAULT now()
);
-- audit log of every blocked duplicate (idea borrowed from the Python OrderSystemPy project)
CREATE TABLE duplicate_attempts (
  id SERIAL PRIMARY KEY, order_id INT, idempotency_key TEXT NOT NULL,
  kind TEXT NOT NULL,            -- DUPLICATE or KEY_REUSED
  customer TEXT, product_id INT, quantity INT, server TEXT,
  attempted_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE outbox (id SERIAL PRIMARY KEY, order_id INT NOT NULL, payload JSONB NOT NULL, sent BOOLEAN DEFAULT FALSE);
CREATE TABLE payments (id SERIAL PRIMARY KEY, order_id INT NOT NULL, amount NUMERIC, created_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE inbox (message_id TEXT PRIMARY KEY, processed_at TIMESTAMPTZ DEFAULT now());
