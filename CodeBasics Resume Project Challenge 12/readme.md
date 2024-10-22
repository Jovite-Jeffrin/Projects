# Insights to Automotive Company on Electric Vehicles launch in India

## Project Overview

AtliQ Motors is an automotive giant from the USA specializing in electric vehicles (EV). In the last 5 years, their market share rose to 25% in electric and hybrid
vehicles segment in North America. As a part of their expansion plans, they wanted to launch their bestselling models in India where their market share is less than 2%. Bruce Haryali, the chief of AtliQ Motors India wanted to do a detailed market study of existing EV/Hybrid market in India before proceeding further. Bruce gave this task to the data analytics team of AtliQ motors and Peter Pandey (me) is the data analyst working in this team.

## Table Creation 

### Create table for sales_by_state

```sql
CREATE TABLE sales_by_state(
	date VARCHAR(30),
	state VARCHAR(50),
	vehicle_category VARCHAR(30),
	electric_vehicles_sold INT,
	total_vehicles_sold INT
);

SELECT * FROM sales_by_state;
```

### Create table for sales_by_makers

```sql
CREATE TABLE sales_by_makers(
	date VARCHAR(30),
	vehicle_category VARCHAR(30),
	maker VARCHAR(50),
	electric_vehicles_sold INT
);

SELECT * FROM sales_by_makers;
```

### Create table for dim_date

```sql
CREATE TABLE dim_date(
	date VARCHAR(30),
	fiscal_year INT,
	quarter VARCHAR(10)
);

SELECT * FROM dim_date;
```

---

### Altering the state name

```sql
UPDATE sales_by_state
SET STATE = 'Andaman & Nicobar Island'
WHERE STATE = 'Andaman & Nicobar';
```

### Creating a primary key and foreign key

```sql
ALTER TABLE dim_date
ADD CONSTRAINT DATE_PK
PRIMARY KEY (date);

ALTER TABLE sales_by_state
ADD CONSTRAINT DATE_FK1
FOREIGN KEY (date)
REFERENCES dim_date(date);

ALTER TABLE sales_by_makers
ADD CONSTRAINT DATE_FK2
FOREIGN KEY (date)
REFERENCES dim_date(date);
```

---

## Preliminary Research Questions

### List the top 3 and bottom 3 makers for the fiscal years 2023 and 2024 in terms of the number of 2-wheelers sold.

```sql
WITH AGGREGATED_VALUES AS (
	SELECT 
		EXTRACT(YEAR FROM SM.date::DATE) AS YEAR, SM.MAKER, SUM(SM.ELECTRIC_VEHICLES_SOLD) AS ELECTRIC_VEHICLES_SOLD
	FROM sales_by_makers SM
	JOIN dim_date DD ON SM.DATE = DD.DATE
	WHERE SM.VEHICLE_CATEGORY = '2-Wheelers' AND DD.FISCAL_YEAR IN (2023,2024)
	GROUP BY 1,2),

TOP3 AS(
	WITH CTE AS (
		SELECT *,
		ROW_NUMBER() OVER(PARTITION BY YEAR ORDER BY ELECTRIC_VEHICLES_SOLD DESC) AS RN
		FROM AGGREGATED_VALUES )
	SELECT YEAR, MAKER, ELECTRIC_VEHICLES_SOLD
	FROM CTE 
	WHERE RN <= 3
),

BOTTOM3 AS(
	WITH CTE AS (
		SELECT *,
		ROW_NUMBER() OVER(PARTITION BY YEAR ORDER BY ELECTRIC_VEHICLES_SOLD) AS RN
		FROM AGGREGATED_VALUES )
	SELECT YEAR, MAKER, ELECTRIC_VEHICLES_SOLD
	FROM CTE 
	WHERE RN <= 3
)

SELECT YEAR,'TOP 3' AS AGG, MAKER, ELECTRIC_VEHICLES_SOLD FROM TOP3
UNION
SELECT YEAR, 'BOTTOM 3' AS AGG, MAKER, ELECTRIC_VEHICLES_SOLD FROM BOTTOM3
ORDER BY YEAR, AGG DESC, ELECTRIC_VEHICLES_SOLD DESC;
```
