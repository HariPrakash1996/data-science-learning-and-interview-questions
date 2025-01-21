
















-------

to find a duplicated and remove then

---duplicates records 

select 

custome_id,

count(*) as rc from 

orders 

having 

count(*) > 1 ;


----removing duplicates from database

select 

distinct 

customer_id from 

order ;

delete from order ( 

select 

custome_id,

count(*) as rc from 

orders 

having 

count(*) > 1 )


--- third highest salary

select salary 

from 

(

select salary , rank() over (partition by emp_id order by salary desc ) as rank )

ranked 

where rank = 3


Shivam soni

3:13 PM

SaleID	SaleDate	Amount

1		2023-12-01	5000

2		2023-12-01	3000

3		2023-12-02	7000

4		2023-12-03	1000

5		2023-12-03	2000


---cummulative sales amount according to the unique slaes date


select 

sum(Amount) over (order by sales_date asc ) rows between 6 preceeding and current row 

from 

sales

;



EmployeeID	EmployeeName	Department	Salary

1			Alice			HR			50000

2			Bob				IT			60000

3			Charlie			IT			70000

4			Diana			HR			40000

5			Eve				IT			80000


----which employee salary is greater than their whole departments salaries

select 

a.Department,

a.avg_sal

from 

(

select 

EmployeeID,

Department,

avg(salary) as avg_sal

from 

employee_table 

) a

where 

a.



SELECT
employee_id,
employee_salary
a.department_id
FROM
employee_table a
INNER JOIN (
SELECT
department_id,
avg(employee_salary) AS department_salary
FROM
employee_table
GROUP BY
department_id
) b
WHERE
employee_salary >= department_salary

explain joins

cummulative salary for unique id 

union vs union all 






------------practice it ----
Question: Write a query to find customers whose total charges exceed $10,000.

Table:
Orders
SELECT customer_id, SUM(amount) AS total_amount
FROM Orders
GROUP BY customer_id
HAVING SUM(amount) > 10000;

Employees Earning More Than Their Manager

Question: Find employees who earn more than their direct managers.

Table:
Employees
emp_id	emp_name	salary	manager_id
1	Alice	8000	NULL
2	Bob	5000	1
3	Charlie	9000	1



SELECT e1.emp_name AS employee, e1.salary, e2.emp_name AS manager, e2.salary AS manager_salary
FROM Employees e1
JOIN Employees e2
ON e1.manager_id = e2.emp_id
WHERE e1.salary > e2.salary;

Difference Between WHERE and HAVING Clauses

Question: Show a query where WHERE filters before grouping and HAVING filters after.

Table:
Orders
order_id	customer_id	amount
1	1	5000
2	1	6000
3	2	3000


-- WHERE filters rows before GROUP BY
SELECT customer_id, SUM(amount) AS total_amount
FROM Orders
WHERE amount > 4000
GROUP BY customer_id
HAVING SUM(amount) > 10000;  -- HAVING filters grouped results


Second Highest Salary

Question: Find the second-highest salary.

Table:
Employees
emp_id
	salary
1	10000
2	8000
3	9000

Solution:

SELECT MAX(salary) AS second_highest_salary
FROM Employees
WHERE salary < (SELECT MAX(salary) FROM Employees);

SELECT salary
FROM (
    SELECT salary, RANK() OVER (ORDER BY salary DESC) AS rank
    FROM Employees
) ranked
WHERE rank = 2;

Average Ratings Per Product Per Month

Question: Calculate the average star ratings for each product on a monthly basis.
SELECT product_id, 
       DATE_FORMAT(review_date, '%Y-%m') AS month, 
       AVG(rating) AS average_rating
FROM Reviews
GROUP BY product_id, DATE_FORMAT(review_date, '%Y-%m');


10. Find Duplicate Rows
SELECT emp_name, salary, COUNT(*) AS duplicate_count
FROM Employees
GROUP BY emp_name, salary
HAVING COUNT(*) > 1;


Question: For each department, find the top 2 highest-paid employees.
SELECT emp_id, emp_name, salary, department
FROM (
    SELECT emp_id, emp_name, salary, department,
           ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rank
    FROM Employees
) ranked
WHERE rank <= 2;


4. Find Customers Who Made Multiple Purchases on the Same Day
SELECT customer_id, transaction_date, COUNT(*) AS transaction_count
FROM Transactions
GROUP BY customer_id, transaction_date
HAVING COUNT(*) > 1;


7. Find the Oldest Employee in Each Department


SELECT emp_id, emp_name, department, birth_date
FROM (
    SELECT emp_id, emp_name, department, birth_date,
           ROW_NUMBER() OVER (PARTITION BY department ORDER BY birth_date ASC) AS rank
    FROM Employees
) ranked
WHERE rank = 1;

8. Cumulative Count of Logins
SELECT user_id, login_date,
       COUNT(*) OVER (PARTITION BY user_id ORDER BY login_date) AS cumulative_logins
FROM Logins;

Delete Duplicate Records

Question: Remove duplicate records but keep the one with the smallest ID.

Table:
Employees
emp_id	emp_name	salary
1	Alice	8000
2	Alice	8000

Solution:

DELETE e1
FROM Employees e1
JOIN Employees e2
ON e1.emp_name = e2.emp_name AND e1.salary = e2.salary
WHERE e1.emp_id > e2.emp_id;


Key Takeaways for Preparation:

    Window Functions: Focus on RANK(), ROW_NUMBER(), and SUM() OVER.
    Joins: Know INNER, LEFT, RIGHT, and FULL OUTER joins thoroughly.
    Removing Duplicates: Understand DISTINCT, DELETE with self-joins, and window functions.
    Cumulative Metrics: Master running totals and cumulative calculations.
    Group Functions: Be confident with GROUP BY, HAVING, and subqueries.



    When you apply a FULL OUTER JOIN between two tables, the resulting row count depends on how many records match between the two tables.

    Table A has 10 records.
    Table B has 16 records.

Full Outer Join Behavior:

    Matching Rows: Rows with matching keys in both tables will appear once.
    Non-Matching Rows: Rows that don’t have a match in the other table will also appear, with NULL values filled in for the columns of the table where there is no match.

Key Scenarios:

    If there is no match between any records in Table A and Table B (a complete disjoint scenario), the result will have 10 + 16 = 26 rows.
    If some rows match between Table A and Table B, the total row count will be less than 26 because matching rows are included only once.

General Formula:
Total Rows=(Rows in Table A without match)+(Matching Rows)+(Rows in Table B without match)
Total Rows=(Rows in Table A without match)+(Matching Rows)+(Rows in Table B without match)
In Your Case:

    Worst Case (No Matching Rows): 10 (from A) + 16 (from B) = 26 rows.
    Best Case (All Rows Match): If all rows in A match rows in B, you will get the greater count of the two tables, which is 16 rows.

Final Answer:

The row count for a FULL OUTER JOIN will be between 16 and 26, depending on how many rows match between Table A and Table B.


How many rows will result from various JOIN operations?
Question:

    Table A has 12 records, and Table B has 15 records.
    a) How many rows will a CROSS JOIN produce?
    b) How many rows will a LEFT JOIN produce if 5 rows from Table A match Table B?
    c) How many rows will a FULL OUTER JOIN produce if 8 rows match?


    Handling NULL values in Joins
Question:

    Table X has 8 rows with some NULL values in the key column.
    Table Y has 10 rows.
    If you perform an INNER JOIN on a column where Table X has NULL values, how many rows will be in the result?

    SELECT a.*
FROM TableA a
LEFT JOIN TableB b
ON a.id = b.id
WHERE b.id IS NULL;



2. Find Consecutive Logins (Streaks)

    Write a query to find users who logged in for N consecutive days.

Hint: Use LAG() or LEAD() to compare rows.
Example:

SELECT user_id, login_date
FROM (
    SELECT user_id, login_date,
           DATEDIFF(login_date, LAG(login_date) OVER (PARTITION BY user_id ORDER BY login_date)) AS day_diff
    FROM Logins
) streaks
WHERE day_diff = 1;




3. Find the Most Frequently Ordered Product

    Write a query to find the most ordered product in the Orders table.

Example:

SELECT product_id, COUNT(*) AS order_count
FROM Orders
GROUP BY product_id
ORDER BY order_count DESC
LIMIT 1;

4. Pivot Table with CASE Statements

    Create a pivot table where sales amounts are categorized by region.

Example:

SELECT product_id,
       SUM(CASE WHEN region = 'North' THEN amount ELSE 0 END) AS North_Sales,
       SUM(CASE WHEN region = 'South' THEN amount ELSE 0 END) AS South_Sales
FROM Sales
GROUP BY product_id;

5. Reverse a String in SQL

    Write a query to reverse a string column in a table.

Example (for MySQL):

SELECT REVERSE('SQLInterview') AS reversed_string;

6. Find the Median Salary

    Write a query to calculate the median salary for employees.

Example:

SELECT AVG(salary) AS median_salary
FROM (
    SELECT salary, ROW_NUMBER() OVER (ORDER BY salary) AS row_num,
                  COUNT(*) OVER () AS total_rows
    FROM Employees
) ranked
WHERE row_num IN (total_rows / 2, (total_rows + 1) / 2);

7. Find Gaps in a Sequence

    Find the missing numbers in a sequence in a table.

Example:

SELECT a.id + 1 AS missing_id
FROM TableA a
LEFT JOIN TableA b
ON a.id + 1 = b.id
WHERE b.id IS NULL;

8. CTE and Recursive Queries

    Write a query to generate a sequence of numbers using a recursive CTE.

Example:

WITH RECURSIVE sequence AS (
    SELECT 1 AS num
    UNION ALL
    SELECT num + 1
    FROM sequence
    WHERE num < 10
)
SELECT num FROM sequence;

9. Rolling Averages

    Calculate a rolling average of sales for the last 7 days.

Example:

SELECT sale_date, 
       AVG(amount) OVER (ORDER BY sale_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg
FROM Sales;

10. Employee-Department Joins

    Find the departments where all employees earn more than a certain threshold.

Example:

SELECT department_id
FROM Employees
GROUP BY department_id
HAVING MIN(salary) > 50000;

11. Find First Purchase for Each Customer

    Write a query to get the first purchase date for each customer.

Example:

SELECT customer_id, MIN(purchase_date) AS first_purchase
FROM Orders
GROUP BY customer_id;

12. Top N Sales Per Region

    Find the top 3 sales transactions in each region.

Example:

SELECT region, sale_id, amount
FROM (
    SELECT region, sale_id, amount,
           ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rank
    FROM Sales
) ranked
WHERE rank <= 3;

13. Find Duplicate Rows Including IDs

    Retrieve duplicate records including the IDs.

Example:

SELECT id, name, salary, COUNT(*) OVER (PARTITION BY name, salary) AS duplicate_count
FROM Employees
HAVING COUNT(*) > 1;

14. Combine Two Tables with Overlapping Data

    Combine rows from two tables where some columns match but not all.

Example (using UNION ALL):

SELECT id, name, salary FROM TableA
UNION ALL
SELECT id, name, salary FROM TableB;

15. Self-Join to Find Employees with Same Salary

    Write a query to find employees who have the same salary.

Example:

SELECT e1.emp_id AS emp1, e2.emp_id AS emp2, e1.salary
FROM Employees e1
JOIN Employees e2
ON e1.salary = e2.salary AND e1.emp_id <> e2.emp_id;

16. Calculate Percentage Contribution

    Write a query to calculate the percentage contribution of each employee’s salary to the total salary.

Example:

SELECT emp_id, salary,
       ROUND((salary / SUM(salary) OVER ()) * 100, 2) AS salary_percentage
FROM Employees;

17. Identify Orphan Records

    Write a query to find rows in Table A that do not have a corresponding match in Table B.

18. Running Totals Reset by Group

    Calculate a running total of sales but reset the total for each region.

Example:

SELECT region, sale_date, amount,
       SUM(amount) OVER (PARTITION BY region ORDER BY sale_date) AS running_total
FROM Sales;

19. Subqueries with NOT IN

    Write a query to find customers who have never placed an order.

Example:

SELECT customer_id
FROM Customers
WHERE customer_id NOT IN (
    SELECT DISTINCT customer_id FROM Orders
);

20. Dynamic Column Aliases

    Demonstrate a query where column aliases are generated dynamically using a function.

21. SQL Query Performance Optimization

    Discuss how you can improve the performance of the following query:

SELECT * FROM Orders WHERE order_date > '2023-01-01';

Key Topics to Focus On

    Joins: Handling NULLs, Multi-table Joins, Self-Join.
    Window Functions: RANK, ROW_NUMBER, SUM(), LAG/LEAD.
    CTEs and Recursive Queries.
    Subqueries: Correlated vs Non-Correlated.
    Performance Optimization: Indexes, Execution Plans.
    Aggregation and GROUP BY.
    Date and Time Functions.
    Pivoting and Unpivoting Data.
    Handling NULL values in calculations and joins.
    Advanced Filtering: HAVING, WHERE, EXISTS, NOT IN.