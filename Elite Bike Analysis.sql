-- creating the database
create database elite_bike_stores;

-- selecting all records from the bike_stores
select * from bike_stores;

-- DATA PREPARATION

-- Handling duplicates data
select 
	ID, `Marital Status`, Gender, Income, 
	Children, Education, Occupation, `Home Owner`, Cars,
    `Commute Distance`, Region, Age, `Purchased Bike`, 
    row_number() over(partition by ID, `Marital Status`, Gender, Income, 
						Children, Education, Occupation, `Home Owner`, Cars,
						`Commute Distance`, Region, Age, `Purchased Bike`) as row_num
FROM bike_stores;

-- creating another table like bike_stores
CREATE TABLE `bike_stores_dup` (
  `ID` int DEFAULT NULL,
  `Marital Status` text,
  `Gender` text,
  `Income` int DEFAULT NULL,
  `Children` int DEFAULT NULL,
  `Education` text,
  `Occupation` text,
  `Home Owner` text,
  `Cars` int DEFAULT NULL,
  `Commute Distance` text,
  `Region` text,
  `Age` int DEFAULT NULL,
  `Purchased Bike` text,
  row_num int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Inserting into the table created
insert into bike_stores_dup
	(select 
	ID, `Marital Status`, Gender, Income, 
	Children, Education, Occupation, `Home Owner`, Cars,
    `Commute Distance`, Region, Age, `Purchased Bike`, 
    row_number() over(partition by ID, `Marital Status`, Gender, Income, 
						Children, Education, Occupation, `Home Owner`, Cars,
						`Commute Distance`, Region, Age, `Purchased Bike`) from bike_stores);
    
-- fetching all records in the bike_stores_dup that are duplicates
select * from bike_stores_dup
where row_num > 1;

-- Deleting the duplicated values
delete from bike_stores_dup
where row_num > 1;

-- fetching the records
select * from bike_stores_dup;

-- dropping the row_num column
alter table bike_stores_dup
drop row_num;

select * from bike_stores_dup;

-- Handling missing values
select missing from
	(select income, coalesce(income, 0, income) as missing
	from bike_stores_dup) as t1
where missing = 0;

select missing from
	(select `Marital Status`, coalesce(`Marital Status`, 'NA', `Marital Status`) as missing
	from bike_stores_dup) as t1
where missing = 'NA';

select missing from
	(select Gender, coalesce(Gender, 'NA', Gender) as missing
	from bike_stores_dup) as t1
where missing = 'NA';

-- CUSTOMER SEGMENTATION
-- Write SQL queries to segment customers based on attributes such as marital status,
	-- income, education, and region.

-- Customer Segmentation by Gender
select gender, count(*) as count_of_gender
from bike_stores_dup
group by gender;

-- Customer Segmentation by Marital status
select `marital status`, count(*) as count_of_status
from bike_stores_dup
group by `Marital Status`;

-- Customer Segmentation by Income
select min(income), max(income) from bike_stores_dup;

select
	case
		WHEN income < 30000 THEN 'Low Income'
        WHEN income BETWEEN 30000 AND 70000 THEN 'Middle Income'
        WHEN income BETWEEN 70000 AND 120000 THEN 'Upper Middle Income'
        ELSE 'High Income'
	end as income_bracket,
    count(*) as count_of_bracket
from bike_stores_dup
group by income_bracket
order by count_of_bracket desc;

-- Customer Segmentation by education
select education, count(*) as education_count
from bike_stores_dup
group by education
order by education_count desc;

-- Customer Segmentation by region
select region, count(*) as region_count
from bike_stores_dup
group by region
order by region_count desc;

-- To segment customers using a combination of 
	-- marital status, income bracket, education level, and region, we can use a more complex query:
SELECT 
    `marital status`,
    CASE 
        WHEN income < 30000 THEN 'Low Income'
        WHEN income BETWEEN 30000 AND 70000 THEN 'Middle Income'
        WHEN income BETWEEN 70000 AND 120000 THEN 'Upper Middle Income'
        ELSE 'High Income'
    END AS income_bracket,
    education,
    region,
    COUNT(*) AS num_customers
FROM  bike_stores_dup
GROUP BY `marital status`, income_bracket, education, region
ORDER BY num_customers DESC;

-- SALES PERFORMANCE ANALYSIS

-- Query the data to calculate total sales, number of bikes purchased, and the 
	-- percentage of customers who purchased a bike.

select * from bike_stores_dup;

-- total sales
select sum(income) as total_income from bike_stores_dup;

-- number of bikes purchased
select count(case when `purchased bike` = 'Yes' then 1 end) 
from bike_stores_dup;

-- or

select `purchased bike`, count(*) as count_of_purchased_bike
from bike_stores_dup
group by `purchased bike`
having `purchased bike` = 'Yes';

-- percentage of customers who purchased a bike.

select 
	round(count(case when `purchased bike` = 'Yes' then 1 end)/
    count(`purchased bike`)  * 100, 2) as percentage_of_customer
from bike_stores_dup;

-- Use aggregated SQL functions (e.g., SUM, AVG, MAX, MIN) to calculate 
	-- average income of bike buyers, the number of children per buyer,  
	-- and other relevant metrics

select * from bike_stores_dup;

-- average income of bike buyers
select avg(income)
from bike_stores_dup
where `purchased bike` = 'Yes';

-- the number of children per buyer
select round(avg(children), 0) as number_of_children_per_buyer
from bike_stores_dup
where `purchased bike` = 'Yes';

-- the number of cars per buyer
select round(avg(cars), 0) as number_of_cars_per_buyer
from bike_stores_dup
where `purchased bike` = 'Yes';

-- Analyze the data by region, occupation, and education level to determine which 
	-- segments generate the most revenue and are the most profitable
select * from bike_stores_dup;


--  Use window functions (e.g., RANK, PARTITION BY) to conduct detailed performance 
	-- ranking of customers or regions.
with ctes as (
select 
	ID, `Marital Status`, Gender, Income, 
	Children, Education, Occupation, `Home Owner`, Cars,
    `Commute Distance`, Region, Age, `Purchased Bike`, 
    rank() over(partition by ID, `Marital Status`, Gender, Income, 
						Children, Education, Occupation, `Home Owner`, Cars,
						`Commute Distance`, Region, Age, `Purchased Bike`) as rank_num
from bike_stores_dup)
select * from ctes;