/* Business query 1: 
Which countries are the company currently operating and which country has the most stores?
 */

SELECT country_code AS country,
COUNT(store_code) AS total_no_stores
FROM dim_store_details
GROUP BY country
ORDER BY total_no_stores DESC;

/* Results for query 1: 
There are three countries: Great Britian, Germany, and the US
The number of stores are: 265, 141, 34 (in corresponding order) 
*/


/* Business query 2: 
Which location has the most stores?
 */

SELECT locality,
COUNT(store_code) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 7;

/* Results for query 2: 
 Chapletown has the most stores (14 stores) 
*/

/* Business query 3: 
 Which months produced the largest amount of sale? 
*/

SELECT month, ROUND(CAST(SUM(o.product_quantity * p.product_price) AS NUMERIC), 2) AS total_sales
FROM orders_table AS o
JOIN dim_date_times d ON o.date_uuid = d.date_uuid
JOIN dim_products p ON o.product_code = p.product_code
GROUP BY month
ORDER BY total_sales DESC;		

/* Results for query 3: 
 Month 8 (August) produced the largest total sales. 
*/

/* Business query 4: 
How many sales are made from online and offline transactions?
*/

ALTER TABLE dim_store_details	
	ADD location VARCHAR(11);

UPDATE dim_store_details
	SET location = 'Online'
	WHERE store_type = 'Web Portal';

UPDATE dim_store_details
	SET location = 'Offline'
	WHERE store_type != 'Web Portal';

WITH online_sales AS(
	SELECT * 
		FROM dim_store_details AS dsd
		INNER JOIN orders_table AS ot ON ot.store_code = dsd.store_code)
		SELECT COUNT(date_uuid) AS number_of_sales,
		SUM(product_quantity) AS product_quantity_count,
			location
		FROM online_sales
		GROUP BY location
		ORDER BY number_of_sales DESC;

/* Results for query 4: 
Less sales were made online than offline. 
Online number of sales: 26,957, product quantity count: 107,739
Offline number of sales: 93,166, product quantity count: 374,047
*/

/* Business query 5: 
What are the percentages of sales that come from each type of store ?
*/

WITH sales_order AS (
	SELECT *
	FROM dim_store_details AS sd
	INNER JOIN orders_table AS ot ON ot.store_code = sd.store_code
	INNER JOIN dim_products AS dp ON dp.product_code = ot.product_code)
		SELECT store_type,
			ROUND(CAST(SUM(product_price * product_quantity)AS NUMERIC), 2) AS total_sales,
			ROUND(CAST(SUM(product_price * product_quantity) * 100.00 / 
			(SELECT SUM(product_price * product_quantity) FROM sales_order)AS NUMERIC), 2) AS percentage_total
		FROM sales_order
		GROUP BY store_type
		ORDER BY percentage_total DESC;

/* Results for query 5: 
Local stores generated the highest precetage of sales (44.55%), followed by:
Web portal (22.36%)
Super store (15.86%)
Mall Kiosk (9.05%)
Outlet (8.19%)
*/


/* Business query 6:  
What month in each year produced the highest total sales?
*/

WITH highest_month_sales_per_year AS(
	SELECT * 
	FROM orders_table AS ot
	INNER JOIN dim_products AS dp ON dp.product_code = ot.product_code
	INNER JOIN dim_date_times AS ddt ON ddt.date_uuid = ot.date_uuid)
		SELECT 
			ROUND(CAST(SUM(product_price * product_quantity)AS NUMERIC),2) AS total_sales,
			year,
			month
		FROM highest_month_sales_per_year
		GROUP BY year, month
		ORDER BY total_sales DESC
		LIMIT 10;

/*Results for query 6: 
The month in which the highest sales was achieved was different in different year. 
For instance, in 1994, the hihgest sale happend in March. 
In 2019, it happned in January.
Further investigation may be needed to see what may be linked to higher sales. 
*/

/* Business query 7:  
What is the  staff headcount in each country?
*/

SELECT SUM(staff_numbers) AS total_staff_count,
	country_code
	FROM dim_store_details
	GROUP BY country_code
	ORDER BY total_staff_count DESC;

/*Results for query 7: 
In the UK, there are 13,132 staff
In Germnay, there are 6054
In the US. there are 1304
*/

/* Business query 8:  
Which store type is selling the most in Germany?
*/ 

WITH german_sales AS (
	SELECT *
	FROM orders_table AS ot
	INNER JOIN dim_products AS dp ON dp.product_code = ot.product_code
	INNER JOIN dim_store_details AS dsd ON dsd.store_code = ot.store_code)
		SELECT 
			ROUND(CAST(SUM(product_price * product_quantity)AS NUMERIC), 2) AS total_sales,
			store_type,
			country_code
		FROM german_sales
		WHERE country_code = 'DE'
		GROUP BY store_type, country_code
		ORDER BY total_sales ASC;

/*Results for query 8: 
Local stores are generating the highest total sales (1104791.74), followed by
Super Store: 383107.31
Mall Kiosk: 246502.79
Outlet: 197516.67
*/

/* Business query 9:  
How quickly is the company making sales per year? 
This is defined as average time taken between each sale
*/

WITH sales_speed AS(
	SELECT TO_TIMESTAMP(CONCAT(day, '-', month, '-', year, ' ', timestamp), 'DD-MM-YYYY HH24:MI:SS') AS datetime,
	year
	FROM dim_date_times
	ORDER BY datetime DESC),
	sales_speed_2 AS(
		SELECT 
			year,
			datetime,
			LEAD(datetime, 1) OVER (ORDER BY datetime DESC) AS time_elapse FROM sales_speed)
			SELECT
				year,
				AVG((datetime - time_elapse)) AS actual_time_taken FROM sales_speed_2
			GROUP BY year
			ORDER BY actual_time_taken DESC 
			limit 5;

/*Results for query 9: 
The most rapid sales were made in 2013, 
with only 2 hours 17 minutes between each sale on average, followed by:
1993: 2 hours 15 mins
2002: 2 hours 13 mins
2022: 2 hours 13 mins
2008: 2 hours 13 mins
*/
