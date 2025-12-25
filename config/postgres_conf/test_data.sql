-- Insert test departments
INSERT INTO departments (department_id, department) VALUES
(1, 'produce'),
(2, 'dairy eggs'),
(3, 'snacks'),
(4, 'beverages'),
(5, 'frozen');

-- Insert test aisles
INSERT INTO aisles (aisle_id, aisle) VALUES
(1, 'fresh fruits'),
(2, 'fresh vegetables'),
(3, 'yogurt'),
(4, 'chips pretzels'),
(5, 'water seltzer sparkling water'),
(6, 'ice cream ice');

-- Insert test products
INSERT INTO products (product_id, product_name, aisle_id, department_id) VALUES
(1, 'Organic Banana', 1, 1),
(2, 'Organic Avocado', 2, 1),
(3, 'Greek Yogurt Plain', 3, 2),
(4, 'Vanilla Ice Cream', 6, 5),
(5, 'Kettle Cooked Chips', 4, 3),
(6, 'Sparkling Water Lime', 5, 4);

-- Insert test orders
INSERT INTO orders (order_id, user_id, order_date) VALUES
(1, 101, '2024-12-01 10:30:00'),
(2, 102, '2024-12-02 14:15:00'),
(3, 101, '2024-12-03 09:45:00'),
(4, 103, '2024-12-04 16:20:00');

-- Insert test order_products
INSERT INTO order_products (order_id, product_id, add_to_cart_order) VALUES
-- Order 1: User 101's first order
(1, 1, 1),  -- Banana first
(1, 3, 2),  -- Yogurt second
(1, 5, 3),  -- Chips third
-- Order 2: User 102's order
(2, 2, 1),  -- Avocado first
(2, 4, 2),  -- Ice cream second
-- Order 3: User 101's second order
(3, 1, 1),  -- Banana again (reorder)
(3, 6, 2),  -- Sparkling water second
(3, 4, 3),  -- Ice cream third
-- Order 4: User 103's order
(4, 2, 1),  -- Avocado
(4, 3, 2),  -- Yogurt
(4, 5, 3);  -- Chips