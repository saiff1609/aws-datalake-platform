-- Q1: Which airlines have the highest average arrival delay?
SELECT airline, avg_arr_delay, total_flights
FROM datalake_flights.avg_delay_by_airline
ORDER BY avg_arr_delay DESC
LIMIT 5;

-- Q2: Which airlines have the highest flight cancellation rate?
SELECT airline, cancellation_rate_pct, total_flights
FROM datalake_flights.cancellation_by_airline
ORDER BY cancellation_rate_pct DESC
LIMIT 5;

-- Q3: What is the biggest cause of flight delays in the US by total minutes lost?
SELECT delay_cause, total_delay_minutes
FROM datalake_flights.delay_cause_breakdown
ORDER BY total_delay_minutes DESC;

-- Q4: Which flight routes have the worst average arrival delays?
SELECT origin, dest, avg_arr_delay, total_flights
FROM datalake_flights.most_delayed_routes
ORDER BY avg_arr_delay DESC
LIMIT 5;

-- Q5: Which months experience the highest average arrival delays?
SELECT month, avg_arr_delay, avg_dep_delay, total_flights
FROM datalake_flights.monthly_delay_trend
ORDER BY avg_arr_delay DESC;