SELECT origin, dest, COUNT (*)
FROM prep_flights
GROUP BY (origin, dest)
ORDER BY origin DESC ;

WITH flight_route_stats AS (SELECT 
								origin, dest, 
								COUNT (flight_number) AS total_flights_route,
								COUNT (DISTINCT tail_number) AS unique_planes,
								COUNT (DISTINCT airline) AS unique_airlines,
								AVG (actual_elapsed_time_interval) AS AVG_actual_elapsed_T,
								AVG (dep_delay_interval) AS dep_arr_delay,
								AVG (arr_delay_interval) AS AVG_arr_delay,
								MAX (arr_delay_interval) AS MAX_arr_delay,
								MIN (arr_delay_interval) AS MIN_arr_delay,
								SUM (cancelled) AS total_cancelled,
								SUM (diverted) AS total_diverted
							FROM {{ref('prep_flights')}}
							GROUP BY (origin, dest)
							ORDER BY origin DESC 
)
SELECT 
		o.city AS origin_city,
		d.city AS dest_city,
		o.country AS origin_country,
		d.country AS dest_country, 
		o.name AS origin_airport,
		d.name AS dest_airport,
		f.*
FROM flight_route_stats f 
LEFT JOIN {{ref('prep_airports')}} o
ON f.origin=o.faa
LEFT JOIN {{ref('prep_airports')}} d
ON f.dest=d.faa