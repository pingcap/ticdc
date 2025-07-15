-- DDL tests for mysql_sink_encoding_test
-- Test various SQL data types and table structures

USE mysql_sink_encoding_test;

-- Test basic data types with primary key
CREATE TABLE basic_types (
    id INT PRIMARY KEY,
    tinyint_col TINYINT,
    smallint_col SMALLINT,
    mediumint_col MEDIUMINT,
    int_col INT,
    bigint_col BIGINT,
    float_col FLOAT,
    double_col DOUBLE,
    decimal_col DECIMAL(10,2),
    char_col CHAR(10),
    varchar_col VARCHAR(255),
    text_col TEXT,
    longtext_col LONGTEXT,
    date_col DATE,
    time_col TIME,
    datetime_col DATETIME,
    timestamp_col TIMESTAMP,
    year_col YEAR,
    boolean_col BOOLEAN,
    bit_col BIT(8)
);

-- Test complex data types with primary key
CREATE TABLE complex_types (
    id INT PRIMARY KEY,
    json_col JSON,
    blob_col BLOB,
    tinyblob_col TINYBLOB,
    mediumblob_col MEDIUMBLOB,
    longblob_col LONGBLOB,
    binary_col BINARY(10),
    varbinary_col VARBINARY(255),
    enum_col ENUM('small', 'medium', 'large'),
    set_col SET('red', 'green', 'blue')
);

-- Test table with multiple primary keys
CREATE TABLE multi_pk_table (
    id1 INT,
    id2 VARCHAR(50),
    name VARCHAR(100),
    value DECIMAL(15,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id1, id2)
);

-- Test table with composite primary key (3 columns)
CREATE TABLE composite_pk_table (
    user_id INT,
    product_id INT,
    order_date DATE,
    quantity INT,
    price DECIMAL(10,2),
    status ENUM('pending', 'processing', 'completed', 'cancelled'),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, product_id, order_date)
);

-- Test table with string-based composite primary key
CREATE TABLE string_composite_pk (
    region VARCHAR(20),
    category VARCHAR(30),
    item_code VARCHAR(50),
    name VARCHAR(100),
    stock_quantity INT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (region, category, item_code)
);

-- Test table with mixed data type composite primary key
CREATE TABLE mixed_pk_table (
    id INT,
    code VARCHAR(20),
    date_part DATE,
    description TEXT,
    amount DECIMAL(15,4),
    is_active BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (id, code, date_part)
);

-- Test table with auto increment
CREATE TABLE auto_increment_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Test table with unique constraints
CREATE TABLE unique_constraints_table (
    id INT PRIMARY KEY,
    email VARCHAR(255) UNIQUE,
    username VARCHAR(50) UNIQUE,
    phone VARCHAR(20),
    UNIQUE KEY unique_phone (phone)
);

-- Test table with foreign key (for referential integrity testing)
CREATE TABLE parent_table (
    parent_id INT PRIMARY KEY,
    parent_name VARCHAR(100) NOT NULL
);

CREATE TABLE child_table (
    child_id INT PRIMARY KEY,
    parent_id INT,
    child_name VARCHAR(100),
    FOREIGN KEY (parent_id) REFERENCES parent_table(parent_id)
);

-- Test table with indexes
CREATE TABLE indexed_table (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(255),
    age INT,
    salary DECIMAL(10,2),
    created_date DATE,
    INDEX idx_name (name),
    INDEX idx_email (email),
    INDEX idx_age_salary (age, salary),
    INDEX idx_created_date (created_date)
);

-- Test table for large volume data
CREATE TABLE large_volume (
    id BIGINT PRIMARY KEY,
    name VARCHAR(100),
    description TEXT,
    value1 DECIMAL(15,4),
    value2 DOUBLE,
    json_data JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_name (name),
    INDEX idx_created_at (created_at)
);

-- Test table with various character sets and collations
CREATE TABLE charset_collation_test (
    id INT PRIMARY KEY,
    utf8_col VARCHAR(100) CHARACTER SET utf8 COLLATE utf8_general_ci,
    utf8mb4_col VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    latin1_col VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    ascii_col VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
);

-- Test table with default values
CREATE TABLE default_values_test (
    id INT PRIMARY KEY,
    name VARCHAR(100) DEFAULT 'unknown',
    age INT DEFAULT 18,
    salary DECIMAL(10,2) DEFAULT 0.00,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Test table with check constraints (if supported)
CREATE TABLE check_constraints_test (
    id INT PRIMARY KEY,
    age INT CHECK (age >= 0 AND age <= 150),
    salary DECIMAL(10,2) CHECK (salary >= 0),
    email VARCHAR(255) CHECK (email LIKE '%@%')
);

-- Finish mark table to indicate DDL completion
CREATE TABLE finish_mark (id INT PRIMARY KEY); 