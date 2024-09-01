-- Composite data of a business organisation, confined to ‘sales and delivery’
-- domain is given for the period of last decade. From the given data retrieve 
-- solutions for the given scenario.
create database miniproject2;
use miniproject2;

-- 1. Join all the tables and create a new table called combined_table.
-- (market_fact, cust_dimen, orders_dimen, prod_dimen, shipping_dimen)

CREATE TABLE combined_table AS
SELECT * FROM(
SELECT mf.Ord_id, mf.Prod_id, mf.Ship_id, mf.Cust_id,mf.Sales,mf.Discount, mf.Order_Quantity,
mf.profit,mf.shipping_cost,mf.Product_Base_Margin, cd.Customer_Name, cd.Province,
cd.Region, cd.Customer_Segment,od.order_id,od.order_date,od.order_priority,pd.product_category,
pd.product_sub_category,sd.ship_mode,sd.ship_date
FROM market_fact mf
INNER JOIN cust_dimen cd ON mf.Cust_id = cd.Cust_id
INNER JOIN orders_dimen od ON od.Ord_id = mf.Ord_id
INNER JOIN prod_dimen pd ON pd.Prod_id = mf.Prod_id
INNER JOIN shipping_dimen sd ON sd.Ship_id = mf.Ship_id
) A;

select * from combined_table;


-- 2. Find the top 3 customers who have the maximum number of orders
SELECT DISTINCT customer_name,sum(order_quantity) over(partition by customer_name) orders 
from COMBINED_TABLE
order by orders desc LIMIT 3;


-- 3. Create a new column DaysTakenForDelivery that contains the date difference 
-- of Order_Date and Ship_Date.
set sql_safe_updates=0;
Alter table combined_table modify DaysTakenForDelivery int;
update combined_table set DaysTakenForDelivery=datediff(Ship_date,order_date);

-- 4. Find the customer whose order took the maximum time to get delivered.
select customer_name, daystakenfordelivery from combined_table where daystakenfordelivery=
(select max(daystakenfordelivery) from combined_table);


-- 5. Retrieve total sales made by each product from the data (use Windows 
-- function)
select distinct prod_id,sum(sales) over(partition by prod_id) total_sales 
from combined_table;


-- 6. Retrieve total profit made from each product from the data (use windows 
-- function)
select distinct prod_id,sum(profit) over(partition by prod_id) total_profit 
from combined_table;


-- 7. Count the total number of unique customers in January and how many of them 
-- came back every month over the entire year in 2011
select distinct year(order_date), month(order_date),count(cust_id) over(partition by month(order_date)) unique_customers from combined_table where cust_id in (
select distinct cust_id from combined_table
where month(order_date)=1 and year(order_date)=2011) and year(order_date)=2011;



-- 8. Retrieve month-by-month customer retention rate since the start of the 
-- business.(using views)
-- Tips:
#1: Create a view where each user’s visits are logged by month, allowing for 
-- the possibility that these will have occurred over multiple # years since 
-- whenever business started operations
# 2: Identify the time lapse between each visit. So, for each person and for each 
-- month, we see when the next visit is.
# 3: Calculate the time gaps between visits
# 4: categorise the customer with time gap 1 as retained, >1 as irregular and 
-- NULL as churned
# 5: calculate the retention month wise

create view v1 as 
select cust_id,order_date, year(order_date) y,month(order_date) m from combined_table order by cust_id;

-- retention rate:
select distinct y,m, count(lap) over(partition by y,m order by y,m) retention from
(select * from
(select *,(m-c1) lap from
(select *,lag(m) over(partition by cust_id,y order by cust_id,y,m) c1 from v1)t1)t2 where lap = 1)t3 ;