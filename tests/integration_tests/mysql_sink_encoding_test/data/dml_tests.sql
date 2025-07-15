-- DML tests for mysql_sink_encoding_test
-- Test various INSERT, UPDATE, DELETE operations with different data types

USE mysql_sink_encoding_test;

-- Insert data into basic_types table
INSERT INTO basic_types VALUES 
(1, 127, 32767, 8388607, 2147483647, 9223372036854775807, 3.14159, 2.718281828, 1234.56, 'char_data', 'varchar_data', 'text_data', 'longtext_data', '2023-01-01', '12:34:56', '2023-01-01 12:34:56', '2023-01-01 12:34:56', 2023, TRUE, b'10101010'),
(2, -128, -32768, -8388608, -2147483648, -9223372036854775808, -3.14159, -2.718281828, -1234.56, 'char_data2', 'varchar_data2', 'text_data2', 'longtext_data2', '2023-12-31', '23:59:59', '2023-12-31 23:59:59', '2023-12-31 23:59:59', 2024, FALSE, b'01010101'),
(3, 0, 0, 0, 0, 0, 0.0, 0.0, 0.00, '', '', '', '', '2023-06-15', '00:00:00', '2023-06-15 00:00:00', '2023-06-15 00:00:00', 2023, FALSE, b'00000000');

-- Insert data into complex_types table
INSERT INTO complex_types VALUES 
(1, '{"name": "John", "age": 30, "city": "New York"}', X'0102030405', X'01', X'0102030405060708', X'0102030405060708090A0B0C0D0E0F', X'0102030405060708090A', X'0102030405060708090A0B0C0D0E0F', 'medium', 'red,green'),
(2, '{"name": "Jane", "age": 25, "city": "Los Angeles"}', X'0A0B0C0D0E', X'02', X'0A0B0C0D0E0F1011', X'0A0B0C0D0E0F101112131415161718', X'0A0B0C0D0E0F10111213', X'0A0B0C0D0E0F101112131415161718', 'large', 'blue,green');

-- Insert data into multi_pk_table
INSERT INTO multi_pk_table VALUES 
(1, 'key1', 'Product A', 100.50, '2023-01-01 10:00:00'),
(1, 'key2', 'Product B', 200.75, '2023-01-01 11:00:00'),
(2, 'key1', 'Product C', 300.25, '2023-01-01 12:00:00'),
(2, 'key2', 'Product D', 400.00, '2023-01-01 13:00:00'),
(3, 'key1', 'Product E', 500.25, '2023-01-01 14:00:00');

-- Insert data into composite_pk_table
INSERT INTO composite_pk_table VALUES 
(1001, 2001, '2023-01-01', 2, 150.00, 'pending', '2023-01-01 09:00:00'),
(1001, 2002, '2023-01-01', 1, 75.50, 'processing', '2023-01-01 09:15:00'),
(1001, 2001, '2023-01-02', 3, 225.00, 'completed', '2023-01-02 10:00:00'),
(1002, 2001, '2023-01-01', 1, 150.00, 'cancelled', '2023-01-01 11:00:00'),
(1002, 2003, '2023-01-01', 5, 500.00, 'pending', '2023-01-01 12:00:00');

-- Insert data into string_composite_pk
INSERT INTO string_composite_pk VALUES 
('North', 'Electronics', 'LAPTOP001', 'Gaming Laptop', 10, '2023-01-01 08:00:00'),
('North', 'Electronics', 'PHONE001', 'Smartphone', 25, '2023-01-01 08:30:00'),
('South', 'Electronics', 'LAPTOP001', 'Business Laptop', 15, '2023-01-01 09:00:00'),
('East', 'Clothing', 'SHIRT001', 'Cotton T-Shirt', 50, '2023-01-01 09:30:00'),
('West', 'Clothing', 'PANTS001', 'Jeans', 30, '2023-01-01 10:00:00');

-- Insert data into mixed_pk_table
INSERT INTO mixed_pk_table VALUES 
(1, 'INV001', '2023-01-01', 'January invoice', 1250.75, TRUE),
(1, 'INV002', '2023-01-01', 'January bonus', 500.00, TRUE),
(2, 'INV001', '2023-01-01', 'February invoice', 1350.25, TRUE),
(2, 'INV001', '2023-01-02', 'February bonus', 600.00, FALSE),
(3, 'INV003', '2023-01-01', 'March invoice', 1100.50, TRUE);

-- Insert data into auto_increment_table
INSERT INTO auto_increment_table (name, description) VALUES 
('Auto Record 1', 'First auto-increment record'),
('Auto Record 2', 'Second auto-increment record'),
('Auto Record 3', 'Third auto-increment record');

-- Insert data into unique_constraints_table
INSERT INTO unique_constraints_table VALUES 
(1, 'user1@example.com', 'user1', '+1234567890'),
(2, 'user2@example.com', 'user2', '+1234567891'),
(3, 'user3@example.com', 'user3', '+1234567892');

-- Insert data into parent_table and child_table
INSERT INTO parent_table VALUES 
(1, 'Parent 1'),
(2, 'Parent 2'),
(3, 'Parent 3');

INSERT INTO child_table VALUES 
(1, 1, 'Child 1 of Parent 1'),
(2, 1, 'Child 2 of Parent 1'),
(3, 2, 'Child 1 of Parent 2'),
(4, 3, 'Child 1 of Parent 3');

-- Insert data into indexed_table
INSERT INTO indexed_table VALUES 
(1, 'John Doe', 'john@example.com', 30, 50000.00, '2023-01-01'),
(2, 'Jane Smith', 'jane@example.com', 25, 60000.00, '2023-01-02'),
(3, 'Bob Johnson', 'bob@example.com', 35, 70000.00, '2023-01-03'),
(4, 'Alice Brown', 'alice@example.com', 28, 55000.00, '2023-01-04');

-- Insert data into charset_collation_test
INSERT INTO charset_collation_test VALUES 
(1, 'UTF8 text', 'UTF8MB4 text with emoji 😀', 'Latin1 text', 'ASCII text'),
(2, 'UTF8 中文', 'UTF8MB4 中文 with emoji 🎉', 'Latin1 special chars', 'ASCII only');

-- Insert data into default_values_test
INSERT INTO default_values_test (id, name, age, salary) VALUES 
(1, 'Custom Name', 25, 50000.00),
(2, DEFAULT, DEFAULT, DEFAULT);

-- Insert data into check_constraints_test
INSERT INTO check_constraints_test VALUES 
(1, 25, 50000.00, 'valid@email.com'),
(2, 0, 0.00, 'another@email.com'),
(3, 150, 999999.99, 'test@domain.org');

-- Test UPDATE operations - Basic types
UPDATE basic_types SET varchar_col = 'updated_varchar', decimal_col = 9999.99 WHERE id = 1;
UPDATE basic_types SET tinyint_col = 100, float_col = 5.678 WHERE id = 2;
UPDATE basic_types SET char_col = 'updated', text_col = 'Updated text content' WHERE id = 3;

-- Test UPDATE operations - Complex types
UPDATE complex_types SET json_col = '{"name": "Updated John", "age": 31, "city": "Boston"}' WHERE id = 1;
UPDATE complex_types SET enum_col = 'large', set_col = 'red,blue' WHERE id = 2;

-- Test UPDATE operations - Multi-primary key tables
UPDATE multi_pk_table SET value = 150.75, name = 'Updated Product A' WHERE id1 = 1 AND id2 = 'key1';
UPDATE multi_pk_table SET value = 250.50 WHERE id1 = 2 AND id2 = 'key1';
UPDATE multi_pk_table SET name = 'Product E Updated' WHERE id1 = 3 AND id2 = 'key1';

-- Test UPDATE operations - Composite primary key tables
UPDATE composite_pk_table SET status = 'completed', quantity = 4 WHERE user_id = 1001 AND product_id = 2001 AND order_date = '2023-01-01';
UPDATE composite_pk_table SET price = 160.00 WHERE user_id = 1001 AND product_id = 2002 AND order_date = '2023-01-01';
UPDATE composite_pk_table SET status = 'processing' WHERE user_id = 1002 AND product_id = 2003 AND order_date = '2023-01-01';

-- Test UPDATE operations - String composite primary key
UPDATE string_composite_pk SET stock_quantity = 8, name = 'Gaming Laptop Pro' WHERE region = 'North' AND category = 'Electronics' AND item_code = 'LAPTOP001';
UPDATE string_composite_pk SET stock_quantity = 20 WHERE region = 'North' AND category = 'Electronics' AND item_code = 'PHONE001';
UPDATE string_composite_pk SET stock_quantity = 45 WHERE region = 'East' AND category = 'Clothing' AND item_code = 'SHIRT001';

-- Test UPDATE operations - Mixed primary key
UPDATE mixed_pk_table SET amount = 1300.00, description = 'Updated January invoice' WHERE id = 1 AND code = 'INV001' AND date_part = '2023-01-01';
UPDATE mixed_pk_table SET is_active = FALSE WHERE id = 2 AND code = 'INV001' AND date_part = '2023-01-02';

-- Test UPDATE operations - Indexed table
UPDATE indexed_table SET salary = 65000.00, age = 31 WHERE id = 1;
UPDATE indexed_table SET email = 'john.updated@example.com' WHERE id = 1;
UPDATE indexed_table SET salary = 72000.00 WHERE id = 2;
UPDATE indexed_table SET age = 36, salary = 75000.00 WHERE id = 3;

-- Test UPDATE operations - Auto increment table
UPDATE auto_increment_table SET description = 'Updated auto-increment record' WHERE id = 1;
UPDATE auto_increment_table SET name = 'Auto Record 2 Updated' WHERE id = 2;

-- Test UPDATE operations - Unique constraints table
UPDATE unique_constraints_table SET email = 'user1.updated@example.com' WHERE id = 1;
UPDATE unique_constraints_table SET phone = '+1234567899' WHERE id = 2;

-- Test UPDATE operations - Parent/Child tables
UPDATE parent_table SET parent_name = 'Updated Parent 1' WHERE parent_id = 1;
UPDATE child_table SET child_name = 'Updated Child 1' WHERE child_id = 1;

-- Test UPDATE operations - Charset collation test
UPDATE charset_collation_test SET utf8_col = 'UTF8 updated', utf8mb4_col = 'UTF8MB4 updated with emoji 🎊' WHERE id = 1;

-- Test UPDATE operations - Default values test
UPDATE default_values_test SET name = 'Custom Updated Name', age = 30 WHERE id = 1;

-- Test DELETE operations - Basic types
DELETE FROM basic_types WHERE id = 4;
DELETE FROM basic_types WHERE id = 6;

-- Test DELETE operations - Complex types
DELETE FROM complex_types WHERE id = 3;

-- Test DELETE operations - Multi-primary key tables
DELETE FROM multi_pk_table WHERE id1 = 1 AND id2 = 'key2';
DELETE FROM multi_pk_table WHERE id1 = 2 AND id2 = 'key2';

-- Test DELETE operations - Composite primary key tables
DELETE FROM composite_pk_table WHERE user_id = 1002 AND product_id = 2001 AND order_date = '2023-01-01';
DELETE FROM composite_pk_table WHERE user_id = 1001 AND product_id = 2002 AND order_date = '2023-01-01';

-- Test DELETE operations - String composite primary key
DELETE FROM string_composite_pk WHERE region = 'West' AND category = 'Clothing' AND item_code = 'PANTS001';
DELETE FROM string_composite_pk WHERE region = 'South' AND category = 'Electronics' AND item_code = 'LAPTOP001';

-- Test DELETE operations - Mixed primary key
DELETE FROM mixed_pk_table WHERE id = 2 AND code = 'INV001' AND date_part = '2023-01-02';
DELETE FROM mixed_pk_table WHERE id = 3 AND code = 'INV003' AND date_part = '2023-01-01';

-- Test DELETE operations - Indexed table
DELETE FROM indexed_table WHERE id = 4;
DELETE FROM indexed_table WHERE id = 3;

-- Test DELETE operations - Auto increment table
DELETE FROM auto_increment_table WHERE id = 3;

-- Test DELETE operations - Unique constraints table
DELETE FROM unique_constraints_table WHERE id = 3;

-- Test DELETE operations - Parent/Child tables
DELETE FROM child_table WHERE child_id = 4;
DELETE FROM child_table WHERE child_id = 2;

-- Test DELETE operations - Charset collation test
DELETE FROM charset_collation_test WHERE id = 3;

-- Test DELETE operations - Default values test
DELETE FROM default_values_test WHERE id = 2;

-- Test DELETE operations - Check constraints test
DELETE FROM check_constraints_test WHERE id = 3;
DELETE FROM check_constraints_test WHERE id = 2;

-- Test INSERT with NULL values
INSERT INTO basic_types (id, tinyint_col, varchar_col) VALUES (4, NULL, NULL);
INSERT INTO complex_types (id, json_col, blob_col) VALUES (3, NULL, NULL);

-- Test INSERT with special characters and Unicode
INSERT INTO charset_collation_test VALUES 
(3, 'UTF8 special: áéíóú', 'UTF8MB4 special: 🚀🌟🎯', 'Latin1: äöüß', 'ASCII: !@#$%');

-- Test INSERT with large text data
INSERT INTO basic_types VALUES 
(5, 100, 1000, 100000, 1000000, 1000000000, 1.23456, 9.87654, 12345.67, 'large_char', 'large_varchar_data_that_is_very_long_and_contains_many_characters_to_test_varchar_handling', 'This is a very long text field that contains a lot of data to test text handling capabilities. It includes multiple sentences and various punctuation marks.', 'This is an extremely long text field that contains a massive amount of data to test longtext handling capabilities. It includes multiple paragraphs, various punctuation marks, and should be able to handle very large amounts of text data without any issues.', '2023-07-15', '15:30:45', '2023-07-15 15:30:45', '2023-07-15 15:30:45', 2023, TRUE, b'11111111');

-- Test INSERT with edge case values
INSERT INTO basic_types VALUES 
(6, 0, 0, 0, 0, 0, 0.0, 0.0, 0.00, '', '', '', '', '0000-00-00', '00:00:00', '0000-00-00 00:00:00', '0000-00-00 00:00:00', 0, FALSE, b'00000000'),
(7, 255, 65535, 16777215, 4294967295, 18446744073709551615, 3.402823E+38, 1.7976931348623157E+308, 9999999999.99, 'max_char', 'max_varchar', 'max_text', 'max_longtext', '9999-12-31', '23:59:59', '9999-12-31 23:59:59', '9999-12-31 23:59:59', 2155, TRUE, b'11111111');

-- ========================================
-- Real-world business scenario simulation
-- ========================================

-- Simulate e-commerce order processing workflow
-- Step 1: Insert new orders
INSERT INTO composite_pk_table VALUES 
(1003, 2004, '2023-01-03', 1, 299.99, 'pending', '2023-01-03 10:00:00'),
(1003, 2005, '2023-01-03', 2, 159.98, 'pending', '2023-01-03 10:05:00'),
(1004, 2001, '2023-01-03', 1, 150.00, 'pending', '2023-01-03 11:00:00');

-- Step 2: Update order status to processing
UPDATE composite_pk_table SET status = 'processing' WHERE user_id = 1003 AND product_id = 2004 AND order_date = '2023-01-03';
UPDATE composite_pk_table SET status = 'processing' WHERE user_id = 1003 AND product_id = 2005 AND order_date = '2023-01-03';

-- Step 3: Update inventory
UPDATE string_composite_pk SET stock_quantity = stock_quantity - 1 WHERE region = 'North' AND category = 'Electronics' AND item_code = 'LAPTOP001';
UPDATE string_composite_pk SET stock_quantity = stock_quantity - 2 WHERE region = 'North' AND category = 'Electronics' AND item_code = 'PHONE001';

-- Step 4: Complete some orders
UPDATE composite_pk_table SET status = 'completed' WHERE user_id = 1003 AND product_id = 2004 AND order_date = '2023-01-03';

-- Step 5: Cancel one order
UPDATE composite_pk_table SET status = 'cancelled' WHERE user_id = 1004 AND product_id = 2001 AND order_date = '2023-01-03';

-- Step 6: Restore inventory for cancelled order
UPDATE string_composite_pk SET stock_quantity = stock_quantity + 1 WHERE region = 'North' AND category = 'Electronics' AND item_code = 'LAPTOP001';

-- Simulate inventory management workflow
-- Step 1: Add new products
INSERT INTO string_composite_pk VALUES 
('Central', 'Electronics', 'TABLET001', 'Tablet Pro', 20, '2023-01-03 14:00:00'),
('Central', 'Clothing', 'JACKET001', 'Winter Jacket', 15, '2023-01-03 14:30:00');

-- Step 2: Update product information
UPDATE string_composite_pk SET name = 'Tablet Pro Max', stock_quantity = 18 WHERE region = 'Central' AND category = 'Electronics' AND item_code = 'TABLET001';

-- Step 3: Remove discontinued products
DELETE FROM string_composite_pk WHERE region = 'West' AND category = 'Clothing' AND item_code = 'PANTS001';

-- Simulate financial transaction workflow
-- Step 1: Insert new invoices
INSERT INTO mixed_pk_table VALUES 
(4, 'INV001', '2023-01-03', 'March invoice', 1200.00, TRUE),
(4, 'INV002', '2023-01-03', 'March bonus', 800.00, TRUE),
(5, 'INV001', '2023-01-03', 'April invoice', 1400.00, TRUE);

-- Step 2: Update invoice amounts
UPDATE mixed_pk_table SET amount = 1250.00 WHERE id = 4 AND code = 'INV001' AND date_part = '2023-01-03';

-- Step 3: Deactivate old invoices
UPDATE mixed_pk_table SET is_active = FALSE WHERE id = 1 AND code = 'INV002' AND date_part = '2023-01-01';

-- Step 4: Delete invalid invoices
DELETE FROM mixed_pk_table WHERE id = 5 AND code = 'INV001' AND date_part = '2023-01-03';

-- Simulate user management workflow
-- Step 1: Add new users
INSERT INTO unique_constraints_table VALUES 
(4, 'user4@example.com', 'user4', '+1234567893'),
(5, 'user5@example.com', 'user5', '+1234567894');

-- Step 2: Update user information
UPDATE unique_constraints_table SET email = 'user4.updated@example.com' WHERE id = 4;
UPDATE unique_constraints_table SET phone = '+1234567895' WHERE id = 5;

-- Step 3: Deactivate users
DELETE FROM unique_constraints_table WHERE id = 1;

-- Simulate employee management workflow
-- Step 1: Add new employees
INSERT INTO indexed_table VALUES 
(5, 'Eva Wilson', 'eva@example.com', 29, 58000.00, '2023-01-03'),
(6, 'Frank Brown', 'frank@example.com', 33, 62000.00, '2023-01-03');

-- Step 2: Update employee information
UPDATE indexed_table SET salary = 60000.00, age = 30 WHERE id = 5;
UPDATE indexed_table SET email = 'frank.brown@example.com' WHERE id = 6;

-- Step 3: Remove terminated employees
DELETE FROM indexed_table WHERE id = 2;

-- Simulate data cleanup operations
-- Remove old records
DELETE FROM basic_types WHERE id = 7;
DELETE FROM complex_types WHERE id = 2;

-- Update remaining records
UPDATE basic_types SET varchar_col = 'final_update' WHERE id = 5;
UPDATE complex_types SET json_col = '{"name": "Final Update", "status": "completed"}' WHERE id = 1;

-- Final insert operations
INSERT INTO basic_types (id, varchar_col, text_col) VALUES 
(8, 'final_insert', 'Final test record for comprehensive testing'),
(9, 'another_final', 'Another final test record');

-- Insert final records for composite keys
INSERT INTO composite_pk_table VALUES 
(1005, 2006, '2023-01-03', 1, 99.99, 'pending', '2023-01-03 16:00:00');

INSERT INTO string_composite_pk VALUES 
('Final', 'Test', 'FINAL001', 'Final Test Product', 1, '2023-01-03 16:30:00');

INSERT INTO mixed_pk_table VALUES 
(6, 'FINAL', '2023-01-03', 'Final test invoice', 999.99, TRUE); 