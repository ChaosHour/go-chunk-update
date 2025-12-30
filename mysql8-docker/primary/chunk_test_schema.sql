-- Test database for go-chunk-update comprehensive testing
CREATE DATABASE IF NOT EXISTS chunk_test;
USE chunk_test;

-- Large table for performance testing (similar to DailyBudgetDetail)
DROP TABLE IF EXISTS large_test_table;
CREATE TABLE large_test_table (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    MCID INT NOT NULL,
    data VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_mcid (MCID),
    INDEX idx_created_at (created_at)
);

-- Insert test data with various MCID values
DELIMITER $$
CREATE PROCEDURE generate_test_data()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE batch_size INT DEFAULT 1000;

    WHILE i <= 50000 DO
        INSERT INTO large_test_table (MCID, data)
        SELECT
            FLOOR(RAND() * 500) + 100,  -- MCID between 100-599
            CONCAT('Test data ', i + j)
        FROM (
            SELECT (a.a + (10 * b.a) + (100 * c.a)) as j
            FROM (SELECT 0 as a UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) as a
            CROSS JOIN (SELECT 0 as a UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) as b
            CROSS JOIN (SELECT 0 as a UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) as c
        ) numbers
        WHERE j < batch_size;

        SET i = i + batch_size;
    END WHILE;
END $$
DELIMITER ;

CALL generate_test_data();
DROP PROCEDURE generate_test_data;

-- Create archive table for testing INSERT operations
DROP TABLE IF EXISTS archive_table;
CREATE TABLE archive_table LIKE large_test_table;

-- Create temp tables for complex operations
DROP TABLE IF EXISTS temp_cleanup;
CREATE TABLE temp_cleanup (id INT PRIMARY KEY);
INSERT INTO temp_cleanup VALUES (1), (3), (5), (7), (9);

-- Create tables for JOIN testing
DROP TABLE IF EXISTS accounts;
CREATE TABLE accounts (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    uuid VARCHAR(36) UNIQUE,
    email VARCHAR(255),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS user_table;
CREATE TABLE user_table (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    consumer_id VARCHAR(36) UNIQUE,
    email_id VARCHAR(255),
    fname VARCHAR(100),
    lname VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert test accounts data
INSERT INTO accounts (uuid, email, first_name, last_name) VALUES
('uuid-1001', 'user1001@example.com', 'John', 'Doe'),
('uuid-1002', 'user1002@example.com', 'Jane', 'Smith'),
('uuid-1003', 'user1003@example.com', 'Bob', 'Johnson'),
('uuid-1004', 'user1004@example.com', 'Alice', 'Williams'),
('uuid-1005', 'user1005@example.com', 'Charlie', 'Brown');

-- Create tables for GDPR testing
DROP TABLE IF EXISTS reservation_logs;
CREATE TABLE reservation_logs (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    reservation_content TEXT,
    user_id BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO reservation_logs (reservation_content, user_id) VALUES
('GDPR sensitive data here', 1),
('More sensitive reservation data', 2),
(NULL, 3),
('Another reservation', 4);

-- Create tables for complex deletion testing
DROP TABLE IF EXISTS unit_price_summaries;
CREATE TABLE unit_price_summaries (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    campaign_membership_coupon_id BIGINT,
    price DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO unit_price_summaries (campaign_membership_coupon_id, price) VALUES
(1, 99.99), (2, 149.99), (3, 79.99), (1, 89.99), (4, 199.99);

-- Create indexes for better performance
CREATE INDEX idx_campaign_coupon ON unit_price_summaries(campaign_membership_coupon_id);

-- Summary
SELECT
    'large_test_table' as table_name,
    COUNT(*) as row_count,
    MIN(MCID) as min_mcid,
    MAX(MCID) as max_mcid
FROM large_test_table
UNION ALL
SELECT
    'accounts' as table_name,
    COUNT(*) as row_count,
    NULL as min_mcid,
    NULL as max_mcid
FROM accounts
UNION ALL
SELECT
    'unit_price_summaries' as table_name,
    COUNT(*) as row_count,
    NULL as min_mcid,
    NULL as max_mcid
FROM unit_price_summaries;