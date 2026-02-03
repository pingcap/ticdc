-- Log redaction integration test data

USE log_redaction_test;

-- Create data model with sensitive fields
CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY,
    username VARCHAR(100),
    email VARCHAR(100),
    password VARCHAR(100),
    credit_card VARCHAR(20),
    ssn VARCHAR(11)
);

-- INSERT operations with sensitive data
INSERT INTO users VALUES
(1, 'user1', 'user1@example.com', 'Password1!', '4532-1000-1000-1000', '100-10-1000'),
(2, 'user2', 'user2@example.com', 'Password2!', '4532-2000-2000-2000', '200-20-2000'),
(3, 'user3', 'user3@example.com', 'Password3!', '4532-3000-3000-3000', '300-30-3000'),
(10, 'user10', 'user10@secret.com', 'SecretPass1!', '1111-2222-3333-4444', '999-88-7777'),
(11, 'user11', 'user11@secret.com', 'SecretPass2!', '5555-6666-7777-8888', '666-55-4444');

-- UPDATE operations
UPDATE users SET password = 'NewPassword1!' WHERE id = 1;
UPDATE users SET credit_card = '9999-1111-1111-1111' WHERE id = 2;
UPDATE users SET password = 'UpdatedSecret!' WHERE id = 10;
UPDATE users SET email = 'newemail@secret.com', credit_card = '0000-1111-2222-3333' WHERE id = 11;

-- DELETE operation
DELETE FROM users WHERE id = 10;

-- DDL operations (not redacted - for debugging)
CREATE INDEX idx_email ON users(email);
ALTER TABLE users ADD COLUMN phone VARCHAR(20);
UPDATE users SET phone = '555-1234-5678' WHERE id = 1;
