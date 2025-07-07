-- Add batch_timestamp column to charges
ALTER TABLE charges ADD COLUMN batch_timestamp TIMESTAMP;

-- Add batch_timestamp column to invoice_line_items
ALTER TABLE invoice_line_items ADD COLUMN batch_timestamp TIMESTAMP;

-- Add batch_timestamp column to prices
ALTER TABLE prices ADD COLUMN batch_timestamp TIMESTAMP;

-- Add batch_timestamp column to products
ALTER TABLE products ADD COLUMN batch_timestamp TIMESTAMP;