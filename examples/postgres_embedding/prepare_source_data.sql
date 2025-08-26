-- Usage: run with psql from your shell, for example:
-- $ psql "postgres://cocoindex:cocoindex@localhost/cocoindex" -f ./prepare_source_data.sql
-- ========================================
-- Simple schema: source_messages (single primary key)
-- ========================================
DROP TABLE IF EXISTS source_messages CASCADE;
CREATE TABLE source_messages (
    id uuid NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    author text NOT NULL,
    message text NOT NULL,
    created_at timestamp DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO source_messages (author, message)
VALUES (
        'Jane Smith',
        'Hello world! This is a test message.'
    ),
    (
        'John Doe',
        'PostgreSQL source integration is working great!'
    ),
    (
        'Jane Smith',
        'CocoIndex makes database processing so much easier.'
    ),
    (
        'John Doe',
        'Embeddings and vector search are powerful tools.'
    ),
    (
        'John Doe',
        'Natural language processing meets database technology.'
    ) ON CONFLICT DO NOTHING;
-- ========================================
-- Multiple schema: source_products (composite primary key)
-- ========================================
DROP TABLE IF EXISTS source_products CASCADE;
CREATE TABLE source_products (
    product_category text NOT NULL,
    product_name text NOT NULL,
    description text,
    price double precision,
    amount integer,
    modified_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_category, product_name)
);
INSERT INTO source_products (
        product_category,
        product_name,
        description,
        price,
        amount,
        modified_time
    )
VALUES (
        'Electronics',
        'Wireless Headphones',
        'High-quality wireless headphones with noise cancellation',
        199.99,
        50,
        NOW() - INTERVAL '3 days'
    ),
    (
        'Electronics',
        'Smartphone',
        'Latest flagship smartphone with advanced camera',
        899.99,
        25,
        NOW() - INTERVAL '12 days'
    ),
    (
        'Electronics',
        'Laptop',
        'High-performance laptop for work and gaming',
        1299.99,
        15,
        NOW() - INTERVAL '20 days'
    ),
    (
        'Appliances',
        'Coffee Maker',
        'Programmable coffee maker with 12-cup capacity',
        89.99,
        30,
        NOW() - INTERVAL '5 days'
    ),
    (
        'Sports',
        'Running Shoes',
        'Lightweight running shoes for daily training',
        129.5,
        60,
        NOW() - INTERVAL '1 day'
    ) ON CONFLICT (product_category, product_name) DO NOTHING;