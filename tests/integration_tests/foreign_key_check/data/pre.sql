use test;
CREATE TABLE departments (
    dept_id INT PRIMARY KEY,
    dept_name VARCHAR(50)
);

CREATE TABLE employees (
    emp_id INT PRIMARY KEY,
    emp_name VARCHAR(50),
    dept_id INT,
    FOREIGN KEY (dept_id) REFERENCES departments(dept_id) -- 外键约束
);

INSERT INTO departments (dept_id, dept_name) VALUES (101, 'HR');

INSERT INTO employees (emp_id, emp_name, dept_id) 
VALUES (1, 'Alice', 999); 