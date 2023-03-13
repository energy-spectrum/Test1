CREATE TABLE IF NOT EXISTS rack (
 rack_id INT NOT NULL,
 rack_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS product (
 product_id INT NOT NULL,
 product_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS product_rack (
 product_id INT NOT NULL,
 rack_id INT NOT NULL,
 is_main BOOL NOT NULL,
 UNIQUE (product_id, rack_id)
);

CREATE TABLE IF NOT EXISTS order_product (
 order_id INT NOT NULL,
 product_id INT NOT NULL,
 quantity INT NOT NULL,
 UNIQUE (order_id, product_id)
);
