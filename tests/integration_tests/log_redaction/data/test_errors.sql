-- Log redaction error scenarios test

USE log_redaction_test;

-- Duplicate key errors with sensitive data
INSERT INTO users VALUES (1, 'dup_user', 'dup@example.com', 'DupPass1!', '8888-111-111-111', '111-11-1111');
