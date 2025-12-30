CREATE DATABASE IF NOT EXISTS chaos;

\u chaos
DROP TABLE IF EXISTS my_table;
DROP TABLE IF EXISTS my_log;
DROP TABLE IF EXISTS employees;
DROP PROCEDURE IF EXISTS my_proc;
DROP PROCEDURE IF EXISTS my_proc_new;
DROP event IF EXISTS my_event;
DROP VIEW IF EXISTS my_view;
DROP VIEW IF EXISTS my_view2;

-- Create a table

CREATE TABLE my_table (col1 INT, col2 INT);

-- Create a table for trigger

CREATE TABLE employees (id INT PRIMARY KEY AUTO_INCREMENT,
                                           name varchar(255),
                                                salary INT DEFAULT 3000);

-- Insert some data

INSERT INTO my_table (col1, col2)
VALUES (1, 2),
       (3, 4),
       (5, 6);

-- Create a stored procedure
DELIMITER $$
CREATE PROCEDURE my_proc()
BEGIN
    SELECT 'Hello, world!' AS message;
END $$
DELIMITER ;

-- Create an event

CREATE event my_event ON schedule EVERY 1 HOUR DO
INSERT INTO my_table (col1, col2)
VALUES (1, 2);

-- Create a view

CREATE VIEW my_view AS
SELECT col1,
       col2
FROM my_table
WHERE col1 > 0;

-- Create a view 2

CREATE DEFINER = `root`@`%` VIEW `my_view2` AS
SELECT col1,
       col2
FROM my_table
WHERE col1 > 0;

-- Create a trigger

CREATE TRIGGER set_default_salary
BEFORE
INSERT ON employees
FOR EACH ROW
SET new.salary = 5000;


SELECT sleep(3);


SELECT ROUTINE_NAME,
       routine_type,
       DEFINER
FROM information_schema.routines
WHERE ROUTINE_SCHEMA = 'chaos'
UNION ALL
SELECT TABLE_NAME,
       'VIEW',
       DEFINER
FROM information_schema.views
WHERE table_schema = 'chaos'
UNION ALL
SELECT TRIGGER_NAME,
       'TRIGGER',
       DEFINER
FROM information_schema.triggers
WHERE TRIGGER_SCHEMA = 'chaos'
UNION ALL
SELECT event_name,
       'EVENT',
       DEFINER
FROM information_schema.events
WHERE event_schema = 'chaos';

