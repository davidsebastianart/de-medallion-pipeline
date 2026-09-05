-- Tabel Dimensi Produk
CREATE TABLE IF NOT EXISTS dim_product (
    stock_code VARCHAR(50) PRIMARY KEY,
    description TEXT
);

-- Tabel Dimensi Pelanggan
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id VARCHAR(50) PRIMARY KEY,
    country VARCHAR(100)
);

-- Tabel Fakta Transaksi Penjualan
CREATE TABLE IF NOT EXISTS fct_sales (
    invoice_no VARCHAR(50),
    stock_code VARCHAR(50),
    customer_id VARCHAR(50),
    invoice_date TIMESTAMP,
    quantity INTEGER,
    unit_price NUMERIC(10, 2),
    total_amount NUMERIC(12, 2)
);