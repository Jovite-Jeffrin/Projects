create database cyclistic_db;
use cyclistic_db;

DROP TABLE CYCLISTIC_DATA;

CREATE TABLE IF NOT EXISTS CYCLISTIC_DATA(
    ride_id VARCHAR(50), 
    rideable_type VARCHAR(30), 
    started_at timestamp,         
    ended_at timestamp,
    member_casual VARCHAR(20) 
);

-- DATA MANIPULATION
ALTER TABLE cyclistic_data
ADD started_date date;

UPDATE cyclistic_data
SET started_date = DATE(started_at);

--     started_day varchar(20)
ALTER TABLE cyclistic_data
ADD started_day char(20);

UPDATE cyclistic_data
SET started_day = TO_CHAR(started_date, 'Day');

--     started_month  
ALTER TABLE cyclistic_data
ADD started_month char(20);

UPDATE cyclistic_data
SET started_month = TO_CHAR(started_date, 'Month');

-- travel_duration
ALTER TABLE cyclistic_data
ADD travel_duration time;

UPDATE cyclistic_data
SET travel_duration = ended_at - started_at;

SELECT * 
FROM CYCLISTIC_DATA
LIMIT 10;

select count(*) from cyclistic_data;

-- RIDES PER WEEKDAY
SELECT STARTED_DAY, COUNT(RIDE_ID) AS NUMBER_OF_RIDES
FROM CYCLISTIC_DATA
GROUP BY 1
ORDER BY 2 DESC;

-- RIDES PER MONTH
SELECT STARTED_MONTH, COUNT(RIDE_ID) AS NUMBER_OF_RIDES
FROM CYCLISTIC_DATA
GROUP BY 1
ORDER BY 2 DESC;

-- RIDES PER SEASON
SELECT
	CASE WHEN STARTED_MONTH IN ('December','January','February') THEN 'WINTER' 
		 WHEN STARTED_MONTH IN ('March','April','May') THEN 'SPRING' 
		 WHEN STARTED_MONTH IN ('June','July','August') THEN 'SUMMER' 
		 ELSE 'AUTUMN'
	END AS SEASON,
    COUNT(RIDE_ID) AS NUMBER_OF_RIDE
FROM CYCLISTIC_DATA
GROUP BY 1
ORDER BY 2 DESC;


WITH CTE AS (
    SELECT
        CASE 
            WHEN STARTED_MONTH IN ('December','January','February') THEN 'WINTER' 
            WHEN STARTED_MONTH IN ('March','April','May') THEN 'SPRING' 
            WHEN STARTED_MONTH IN ('June','July','August') THEN 'SUMMER' 
            ELSE 'AUTUMN'
        END AS SEASON,
        COUNT(RIDE_ID) AS NUMBER_OF_RIDE
    FROM CYCLISTIC_DATA
    GROUP BY 1
)
SELECT 
    SEASON, 
    NUMBER_OF_RIDE, 
    ROUND((NUMBER_OF_RIDE::float / total_ride_count * 100):: NUMERIC, 2) AS PERCENT_VALUE
FROM 
    CTE,
    (SELECT COUNT(RIDE_ID) AS total_ride_count FROM CYCLISTIC_DATA) AS total_rides
ORDER BY 
    2 DESC, 3;


-- MOST USED BIKE
SELECT RIDEABLE_TYPE, COUNT(RIDE_ID) AS NUMBER_OF_RIDE
FROM CYCLISTIC_DATA
GROUP BY 1
ORDER BY 2 DESC;

SELECT RIDEABLE_TYPE, MEMBER_CASUAL, COUNT(RIDE_ID) AS NUMBER_OF_RIDE
FROM CYCLISTIC_DATA
GROUP BY 1, 2
ORDER BY 3 DESC;

-- MOST USED BIKE
WITH CTE AS (
	SELECT RIDEABLE_TYPE, COUNT(RIDE_ID) AS NUMBER_OF_RIDE
	FROM CYCLISTIC_DATA
	GROUP BY 1
	ORDER BY 2 DESC)
SELECT 
	RIDEABLE_TYPE, NUMBER_OF_RIDE, 
	ROUND((NUMBER_OF_RIDE :: NUMERIC/(SELECT COUNT(RIDE_ID) FROM CYCLISTIC_DATA))*100,2) PERCENT_VALUE
FROM CTE
ORDER BY 2 DESC, 3;


WITH CTE AS (
	SELECT RIDEABLE_TYPE, MEMBER_CASUAL, COUNT(RIDE_ID) AS NUMBER_OF_RIDE
	FROM CYCLISTIC_DATA
	GROUP BY 1, 2
	ORDER BY 3 DESC)
SELECT 
	RIDEABLE_TYPE, MEMBER_CASUAL, NUMBER_OF_RIDE, 
	ROUND((NUMBER_OF_RIDE :: NUMERIC/(SELECT COUNT(RIDE_ID) FROM CYCLISTIC_DATA))*100,2) PERCENT_VALUE
FROM CTE
ORDER BY 3 DESC, 4;

-- PERCENTAGE OF MEMBERS TYPE
SELECT MEMBER_CASUAL, COUNT(RIDE_ID) AS NUMBER_OF_RIDE
FROM CYCLISTIC_DATA
GROUP BY 1
ORDER BY 2 DESC;

-- PERCENTAGE OF MEMBERS TYPE
WITH CTE AS (
		SELECT MEMBER_CASUAL, COUNT(RIDE_ID) AS NUMBER_OF_RIDE
		FROM CYCLISTIC_DATA
		GROUP BY 1
		ORDER BY 2 DESC)
SELECT 
	MEMBER_CASUAL, NUMBER_OF_RIDE, 
	ROUND((NUMBER_OF_RIDE :: NUMERIC/(SELECT COUNT(RIDE_ID) FROM CYCLISTIC_DATA))*100,2) PERCENT_VALUE
FROM CTE
ORDER BY 2 DESC, 3;

-- AVERAGE TRAVEL DURATION
SELECT member_casual, AVG(TRAVEL_DURATION)
FROM CYCLISTIC_DATA
GROUP BY 1;

SELECT RIDEABLE_TYPE, member_casual, AVG(TRAVEL_DURATION)
FROM CYCLISTIC_DATA
GROUP BY 1,2
order by 3 desc;