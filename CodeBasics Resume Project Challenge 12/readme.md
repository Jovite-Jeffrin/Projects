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
